//! Configuration management for IPMsg-Torrent
//! Supports loading from config.toml files and environment variables

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Network configuration
    #[serde(default)]
    pub network: NetworkConfig,
    
    /// Storage configuration
    #[serde(default)]
    pub storage: StorageConfig,
    
    /// UI configuration
    #[serde(default)]
    pub ui: UiConfig,
    
    /// Download configuration
    #[serde(default)]
    pub download: DownloadConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            ui: UiConfig::default(),
            download: DownloadConfig::default(),
        }
    }
}

/// Network-related configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// TCP/UDP port to listen on (0 = random)
    #[serde(default = "default_port")]
    pub port: u16,
    
    /// Bootstrap nodes to connect to
    #[serde(default)]
    pub bootstrap_nodes: Vec<String>,
    
    /// Enable mDNS for local peer discovery
    #[serde(default = "default_true")]
    pub enable_mdns: bool,
    
    /// Enable relay client
    #[serde(default = "default_true")]
    pub enable_relay: bool,
    
    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub connection_timeout: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            port: 0,
            bootstrap_nodes: vec![
                "/dns4/bootstrap1.libp2p.io/tcp/4001/p2p/QmNnooDu7bfjPFoVaZY5cukYfR3oKQeRgZp3zWzrKzGVyP".to_string(),
                "/dns4/bootstrap2.libp2p.io/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb".to_string(),
            ],
            enable_mdns: true,
            enable_relay: true,
            connection_timeout: 30,
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Data directory path
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    
    /// Maximum message history to keep
    #[serde(default = "default_max_history")]
    pub max_history: usize,
    
    /// Enable message deduplication
    #[serde(default = "default_true")]
    pub enable_dedup: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            max_history: 10000,
            enable_dedup: true,
        }
    }
}

/// UI configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiConfig {
    /// Default username
    #[serde(default = "default_username")]
    pub username: String,
    
    /// Theme (dark/light)
    #[serde(default = "default_theme")]
    pub theme: String,
    
    /// Show timestamps in chat
    #[serde(default = "default_true")]
    pub show_timestamps: bool,
    
    /// Show peer IDs in chat
    #[serde(default)]
    pub show_peer_ids: bool,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            username: "Anonymous".to_string(),
            theme: "dark".to_string(),
            show_timestamps: true,
            show_peer_ids: false,
        }
    }
}

/// Download configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadConfig {
    /// Maximum concurrent downloads
    #[serde(default = "default_max_downloads")]
    pub max_concurrent: usize,
    
    /// Download directory
    #[serde(default = "default_download_dir")]
    pub download_dir: PathBuf,
    
    /// Enable P2SP (multi-source download)
    #[serde(default = "default_true")]
    pub enable_p2sp: bool,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 5,
            download_dir: default_download_dir(),
            enable_p2sp: true,
        }
    }
}

// Default value functions
fn default_port() -> u16 { 0 }
fn default_true() -> bool { true }
fn default_timeout() -> u64 { 30 }
fn default_max_history() -> usize { 10000 }
fn default_username() -> String { "Anonymous".to_string() }
fn default_theme() -> String { "dark".to_string() }
fn default_max_downloads() -> usize { 5 }

fn default_data_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ipmsg-torrent")
}

fn default_download_dir() -> PathBuf {
    default_data_dir().join("downloads")
}

impl Config {
    /// Load configuration from file
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(e.to_string()))?;
        
        toml::from_str(&content)
            .map_err(|e| ConfigError::Parse(e.to_string()))
    }
    
    /// Save configuration to file
    pub fn save(&self, path: &Path) -> Result<(), ConfigError> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| ConfigError::Serialize(e.to_string()))?;
        
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| ConfigError::Io(e.to_string()))?;
        }
        
        std::fs::write(path, content)
            .map_err(|e| ConfigError::Io(e.to_string()))
    }
    
    /// Load from default location or create default config
    pub fn load_or_default() -> Self {
        let config_path = Self::default_config_path();
        Self::load(&config_path).unwrap_or_default()
    }
    
    /// Get default config file path
    pub fn default_config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("ipmsg-torrent")
            .join("config.toml")
    }
    
    /// Override config with environment variables
    pub fn apply_env_overrides(&mut self) {
        if let Ok(port) = std::env::var("IPMSG_PORT") {
            if let Ok(p) = port.parse() {
                self.network.port = p;
            }
        }
        
        if let Ok(username) = std::env::var("IPMSG_USERNAME") {
            self.ui.username = username;
        }
        
        if let Ok(data_dir) = std::env::var("IPMSG_DATA_DIR") {
            self.storage.data_dir = PathBuf::from(data_dir);
        }
    }
}

/// Configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Serialize error: {0}")]
    Serialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.network.port, 0);
        assert!(config.network.enable_mdns);
        assert_eq!(config.ui.username, "Anonymous");
    }
    
    #[test]
    fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("test_config.toml");
        
        let mut config = Config::default();
        config.network.port = 4242;
        config.ui.username = "TestUser".to_string();
        
        config.save(&config_path).unwrap();
        
        let loaded = Config::load(&config_path).unwrap();
        assert_eq!(loaded.network.port, 4242);
        assert_eq!(loaded.ui.username, "TestUser");
    }
    
    #[test]
    fn test_env_overrides() {
        let mut config = Config::default();
        // SAFETY: test-only, single-threaded env mutation
        unsafe {
            std::env::set_var("IPMSG_PORT", "8080");
            std::env::set_var("IPMSG_USERNAME", "EnvUser");
        }
        
        config.apply_env_overrides();
        
        assert_eq!(config.network.port, 8080);
        assert_eq!(config.ui.username, "EnvUser");
        
        // Clean up
        unsafe {
            std::env::remove_var("IPMSG_PORT");
            std::env::remove_var("IPMSG_USERNAME");
        }
    }
}
