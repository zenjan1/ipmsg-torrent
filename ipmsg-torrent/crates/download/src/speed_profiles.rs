//! Download Speed Profiles - Named speed limit presets for quick switching.
//!
//! Users can create named profiles (e.g., "Night Unlimited", "Work 1MB/s", "Game 512KB/s")
//! and switch between them instantly via CLI or REST API. The active profile's speed limit
//! is applied as the global download rate limit.
//!
//! Features:
//! - Create named profiles with speed limits and optional descriptions
//! - Quick-switch between profiles via CLI/API
//! - Track which profile is currently active
//! - Profiles persist to disk and restore on startup
//! - Supports unlimited speed (0 = no limit) per profile

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

/// Error type for speed profile operations.
#[derive(Debug, thiserror::Error)]
pub enum SpeedProfileError {
    #[error("profile not found: {0}")]
    NotFound(String),
    #[error("profile name already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid profile name: {0}")]
    InvalidName(String),
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// A named speed limit preset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedProfile {
    /// Unique identifier (lowercase, no spaces)
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// Download speed limit in bytes per second (0 = unlimited)
    pub speed_limit_bps: u64,
    /// Upload speed limit in bytes per second (0 = unlimited)
    pub upload_limit_bps: u64,
    /// Maximum concurrent downloads for this profile (0 = unlimited)
    pub max_concurrent: u32,
    /// When this profile was created
    pub created_at: DateTime<Local>,
    /// When this profile was last used (switched to)
    pub last_used_at: Option<DateTime<Local>>,
    /// Number of times this profile has been activated
    pub use_count: u64,
}

/// Configuration for the speed profiles system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedProfilesConfig {
    /// All defined profiles
    pub profiles: Vec<SpeedProfile>,
    /// ID of the currently active profile (None = no profile active)
    pub active_profile_id: Option<String>,
    /// Maximum number of profiles allowed (0 = unlimited)
    pub max_profiles: usize,
}

impl Default for SpeedProfilesConfig {
    fn default() -> Self {
        Self {
            profiles: Vec::new(),
            active_profile_id: None,
            max_profiles: 50,
        }
    }
}

/// Summary of the speed profiles system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedProfilesSummary {
    /// Total number of profiles
    pub total_profiles: usize,
    /// Currently active profile (if any)
    pub active_profile: Option<SpeedProfileInfo>,
    /// List of all profiles with basic info
    pub profiles: Vec<SpeedProfileInfo>,
}

/// Basic info about a profile for listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedProfileInfo {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub speed_limit_bps: u64,
    pub upload_limit_bps: u64,
    pub max_concurrent: u32,
    pub is_active: bool,
    pub use_count: u64,
    pub last_used_at: Option<DateTime<Local>>,
    pub created_at: DateTime<Local>,
}

impl SpeedProfile {
    /// Create a new speed profile.
    pub fn new(id: String, name: String, speed_limit_bps: u64) -> Self {
        Self {
            id,
            name,
            description: None,
            speed_limit_bps,
            upload_limit_bps: 0,
            max_concurrent: 0,
            created_at: Local::now(),
            last_used_at: None,
            use_count: 0,
        }
    }

    /// Convert to info struct for listing.
    pub fn to_info(&self, is_active: bool) -> SpeedProfileInfo {
        SpeedProfileInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            speed_limit_bps: self.speed_limit_bps,
            upload_limit_bps: self.upload_limit_bps,
            max_concurrent: self.max_concurrent,
            is_active,
            use_count: self.use_count,
            last_used_at: self.last_used_at,
            created_at: self.created_at,
        }
    }

    /// Format speed limit as human-readable string.
    pub fn format_speed_limit(&self) -> String {
        format_speed_bps(self.speed_limit_bps)
    }
}

/// Format bytes per second as human-readable string.
pub fn format_speed_bps(bps: u64) -> String {
    if bps == 0 {
        "Unlimited".to_string()
    } else if bps >= 1_000_000 {
        format!("{:.1} MB/s", bps as f64 / 1_000_000.0)
    } else if bps >= 1_000 {
        format!("{:.1} KB/s", bps as f64 / 1_000.0)
    } else {
        format!("{} B/s", bps)
    }
}

/// Parse a human-readable speed string to bytes per second.
///
/// Supports formats like: "1MB/s", "512KB/s", "100B/s", "0", "unlimited", "none"
pub fn parse_speed_bps(input: &str) -> Result<u64, SpeedProfileError> {
    let trimmed = input.trim().to_lowercase();

    if trimmed.is_empty() || trimmed == "0" || trimmed == "unlimited" || trimmed == "none" {
        return Ok(0);
    }

    let (num_str, multiplier) = if trimmed.ends_with("mb/s") || trimmed.ends_with("mbps") {
        (&trimmed[..trimmed.len() - 4], 1_000_000u64)
    } else if trimmed.ends_with("kb/s") || trimmed.ends_with("kbps") {
        (&trimmed[..trimmed.len() - 4], 1_000u64)
    } else if trimmed.ends_with("b/s") || trimmed.ends_with("bps") {
        (&trimmed[..trimmed.len() - 3], 1u64)
    } else if trimmed.ends_with('m') {
        (&trimmed[..trimmed.len() - 1], 1_000_000u64)
    } else if trimmed.ends_with('k') {
        (&trimmed[..trimmed.len() - 1], 1_000u64)
    } else {
        // Try parsing as raw number
        (&trimmed[..], 1u64)
    };

    let num_str = num_str.trim();
    let value: f64 = num_str
        .parse()
        .map_err(|_| SpeedProfileError::InvalidName(format!("invalid speed value: {}", input)))?;

    if value < 0.0 {
        return Err(SpeedProfileError::InvalidName(format!(
            "speed cannot be negative: {}",
            input
        )));
    }

    Ok((value * multiplier as f64) as u64)
}

/// Generate a profile ID from a name.
pub fn profile_id_from_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

/// Manager for speed profiles.
#[derive(Debug)]
pub struct SpeedProfileManager {
    config: SpeedProfilesConfig,
    config_path: std::path::PathBuf,
}

impl SpeedProfileManager {
    /// Create a new manager with default config.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            config: SpeedProfilesConfig::default(),
            config_path: data_dir.join("speed_profiles.json"),
        }
    }

    /// Load configuration from disk.
    pub async fn load(&mut self) -> Result<(), SpeedProfileError> {
        match load_speed_profiles_config(&self.config_path).await {
            Ok(config) => {
                self.config = config;
                Ok(())
            }
            Err(SpeedProfileError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                // No config file yet, use defaults
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Save configuration to disk.
    pub async fn save(&self) -> Result<(), SpeedProfileError> {
        save_speed_profiles_config(&self.config, &self.config_path).await
    }

    /// Get the current configuration.
    pub fn config(&self) -> &SpeedProfilesConfig {
        &self.config
    }

    /// Get a summary of all profiles.
    pub fn summary(&self) -> SpeedProfilesSummary {
        let active_id = self.config.active_profile_id.as_deref();
        SpeedProfilesSummary {
            total_profiles: self.config.profiles.len(),
            active_profile: self
                .config
                .profiles
                .iter()
                .find(|p| active_id == Some(p.id.as_str()))
                .map(|p| p.to_info(true)),
            profiles: self
                .config
                .profiles
                .iter()
                .map(|p| p.to_info(active_id == Some(p.id.as_str())))
                .collect(),
        }
    }

    /// Get the currently active profile.
    pub fn active_profile(&self) -> Option<&SpeedProfile> {
        let active_id = self.config.active_profile_id.as_deref()?;
        self.config.profiles.iter().find(|p| p.id == active_id)
    }

    /// Get a profile by ID.
    pub fn get_profile(&self, id: &str) -> Option<&SpeedProfile> {
        self.config.profiles.iter().find(|p| p.id == id)
    }

    /// Create a new speed profile.
    pub async fn create_profile(
        &mut self,
        name: &str,
        speed_limit_bps: u64,
        description: Option<&str>,
    ) -> Result<String, SpeedProfileError> {
        let id = profile_id_from_name(name);
        if id.is_empty() {
            return Err(SpeedProfileError::InvalidName(
                "profile name must contain alphanumeric characters".to_string(),
            ));
        }

        if self.config.profiles.iter().any(|p| p.id == id) {
            return Err(SpeedProfileError::AlreadyExists(name.to_string()));
        }

        if self.config.max_profiles > 0 && self.config.profiles.len() >= self.config.max_profiles {
            return Err(SpeedProfileError::InvalidName(format!(
                "maximum number of profiles ({}) reached",
                self.config.max_profiles
            )));
        }

        let mut profile = SpeedProfile::new(id.clone(), name.to_string(), speed_limit_bps);
        profile.description = description.map(|s| s.to_string());

        self.config.profiles.push(profile);
        self.save().await?;

        Ok(id)
    }

    /// Delete a speed profile.
    pub async fn delete_profile(&mut self, id: &str) -> Result<(), SpeedProfileError> {
        let initial_len = self.config.profiles.len();
        self.config.profiles.retain(|p| p.id != id);

        if self.config.profiles.len() == initial_len {
            return Err(SpeedProfileError::NotFound(id.to_string()));
        }

        // If this was the active profile, deactivate
        if self.config.active_profile_id.as_deref() == Some(id) {
            self.config.active_profile_id = None;
        }

        self.save().await?;
        Ok(())
    }

    /// Activate a speed profile by ID.
    ///
    /// Returns the activated profile's speed limit and max concurrent values.
    pub async fn activate_profile(
        &mut self,
        id: &str,
    ) -> Result<(u64, u64, u32), SpeedProfileError> {
        let profile = self
            .config
            .profiles
            .iter_mut()
            .find(|p| p.id == id)
            .ok_or_else(|| SpeedProfileError::NotFound(id.to_string()))?;

        profile.use_count += 1;
        profile.last_used_at = Some(Local::now());

        let speed_limit = profile.speed_limit_bps;
        let upload_limit = profile.upload_limit_bps;
        let max_concurrent = profile.max_concurrent;

        self.config.active_profile_id = Some(id.to_string());
        self.save().await?;

        Ok((speed_limit, upload_limit, max_concurrent))
    }

    /// Deactivate the current profile (no profile active).
    pub async fn deactivate_profile(&mut self) -> Result<(), SpeedProfileError> {
        self.config.active_profile_id = None;
        self.save().await
    }

    /// Update an existing profile's settings.
    pub async fn update_profile(
        &mut self,
        id: &str,
        speed_limit_bps: Option<u64>,
        upload_limit_bps: Option<u64>,
        max_concurrent: Option<u32>,
        description: Option<&str>,
    ) -> Result<(), SpeedProfileError> {
        let profile = self
            .config
            .profiles
            .iter_mut()
            .find(|p| p.id == id)
            .ok_or_else(|| SpeedProfileError::NotFound(id.to_string()))?;

        if let Some(speed) = speed_limit_bps {
            profile.speed_limit_bps = speed;
        }
        if let Some(upload) = upload_limit_bps {
            profile.upload_limit_bps = upload;
        }
        if let Some(max_c) = max_concurrent {
            profile.max_concurrent = max_c;
        }
        if let Some(desc) = description {
            profile.description = Some(desc.to_string());
        }

        self.save().await?;
        Ok(())
    }

    /// List all profiles sorted by name.
    pub fn list_profiles(&self) -> Vec<&SpeedProfile> {
        let mut profiles: Vec<_> = self.config.profiles.iter().collect();
        profiles.sort_by(|a, b| a.name.cmp(&b.name));
        profiles
    }
}

/// Save speed profiles config to disk atomically.
async fn save_speed_profiles_config(
    config: &SpeedProfilesConfig,
    path: &Path,
) -> Result<(), SpeedProfileError> {
    let json = serde_json::to_string_pretty(config)?;
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(parent).await?;

    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, &json).await?;
    fs::rename(&tmp_path, path).await?;

    Ok(())
}

/// Load speed profiles config from disk.
async fn load_speed_profiles_config(path: &Path) -> Result<SpeedProfilesConfig, SpeedProfileError> {
    let content = fs::read_to_string(path).await?;
    let config: SpeedProfilesConfig = serde_json::from_str(&content)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(0), "Unlimited");
        assert_eq!(format_speed_bps(500), "500 B/s");
        assert_eq!(format_speed_bps(1_000), "1.0 KB/s");
        assert_eq!(format_speed_bps(1_500_000), "1.5 MB/s");
        assert_eq!(format_speed_bps(512_000), "512.0 KB/s");
        assert_eq!(format_speed_bps(10_000_000), "10.0 MB/s");
    }

    #[test]
    fn test_parse_speed_bps() {
        assert_eq!(parse_speed_bps("0").unwrap(), 0);
        assert_eq!(parse_speed_bps("unlimited").unwrap(), 0);
        assert_eq!(parse_speed_bps("none").unwrap(), 0);
        assert_eq!(parse_speed_bps("").unwrap(), 0);
        assert_eq!(parse_speed_bps("1MB/s").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("512KB/s").unwrap(), 512_000);
        assert_eq!(parse_speed_bps("100B/s").unwrap(), 100);
        assert_eq!(parse_speed_bps("5m").unwrap(), 5_000_000);
        assert_eq!(parse_speed_bps("256k").unwrap(), 256_000);
        assert_eq!(parse_speed_bps("1000000").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("1.5MB/s").unwrap(), 1_500_000);
    }

    #[test]
    fn test_parse_speed_bps_errors() {
        assert!(parse_speed_bps("abc").is_err());
        assert!(parse_speed_bps("-1MB/s").is_err());
    }

    #[test]
    fn test_profile_id_from_name() {
        assert_eq!(profile_id_from_name("Night Unlimited"), "night_unlimited");
        assert_eq!(profile_id_from_name("Work 1MB/s"), "work_1mb_s");
        assert_eq!(profile_id_from_name("  Hello World  "), "hello_world");
        assert_eq!(profile_id_from_name("test!!!"), "test");
    }

    #[test]
    fn test_speed_profile_new() {
        let profile = SpeedProfile::new("test".to_string(), "Test Profile".to_string(), 1_000_000);
        assert_eq!(profile.id, "test");
        assert_eq!(profile.name, "Test Profile");
        assert_eq!(profile.speed_limit_bps, 1_000_000);
        assert_eq!(profile.upload_limit_bps, 0);
        assert_eq!(profile.max_concurrent, 0);
        assert_eq!(profile.use_count, 0);
        assert!(profile.last_used_at.is_none());
        assert!(profile.description.is_none());
    }

    #[test]
    fn test_speed_profile_format() {
        let profile = SpeedProfile::new("test".to_string(), "Test".to_string(), 0);
        assert_eq!(profile.format_speed_limit(), "Unlimited");

        let profile = SpeedProfile::new("test".to_string(), "Test".to_string(), 1_500_000);
        assert_eq!(profile.format_speed_limit(), "1.5 MB/s");
    }

    #[test]
    fn test_speed_profile_to_info() {
        let profile = SpeedProfile::new("test".to_string(), "Test".to_string(), 1_000_000);
        let info = profile.to_info(true);
        assert_eq!(info.id, "test");
        assert!(info.is_active);
        assert_eq!(info.speed_limit_bps, 1_000_000);
    }

    #[test]
    fn test_default_config() {
        let config = SpeedProfilesConfig::default();
        assert!(config.profiles.is_empty());
        assert!(config.active_profile_id.is_none());
        assert_eq!(config.max_profiles, 50);
    }

    #[tokio::test]
    async fn test_create_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr
            .create_profile("Night Unlimited", 0, Some("No speed limit at night"))
            .await
            .unwrap();
        assert_eq!(id, "night_unlimited");
        assert_eq!(mgr.config().profiles.len(), 1);

        let profile = mgr.get_profile(&id).unwrap();
        assert_eq!(profile.name, "Night Unlimited");
        assert_eq!(profile.speed_limit_bps, 0);
        assert_eq!(
            profile.description.as_deref(),
            Some("No speed limit at night")
        );
    }

    #[tokio::test]
    async fn test_create_duplicate_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        mgr.create_profile("Test", 1000, None).await.unwrap();
        let result = mgr.create_profile("Test", 2000, None).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SpeedProfileError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_create_invalid_name() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let result = mgr.create_profile("!!!", 1000, None).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SpeedProfileError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_delete_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        assert_eq!(mgr.config().profiles.len(), 1);

        mgr.delete_profile(&id).await.unwrap();
        assert_eq!(mgr.config().profiles.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let result = mgr.delete_profile("nonexistent").await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SpeedProfileError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete_active_profile_deactivates() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        mgr.activate_profile(&id).await.unwrap();
        assert!(mgr.config().active_profile_id.is_some());

        mgr.delete_profile(&id).await.unwrap();
        assert!(mgr.config().active_profile_id.is_none());
    }

    #[tokio::test]
    async fn test_activate_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Work", 1_000_000, None).await.unwrap();
        let (speed, upload, max_c) = mgr.activate_profile(&id).await.unwrap();
        assert_eq!(speed, 1_000_000);
        assert_eq!(upload, 0);
        assert_eq!(max_c, 0);

        let profile = mgr.get_profile(&id).unwrap();
        assert_eq!(profile.use_count, 1);
        assert!(profile.last_used_at.is_some());
        assert_eq!(mgr.config().active_profile_id.as_deref(), Some(id.as_str()));
    }

    #[tokio::test]
    async fn test_activate_nonexistent_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let result = mgr.activate_profile("nonexistent").await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SpeedProfileError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_deactivate_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        mgr.activate_profile(&id).await.unwrap();
        assert!(mgr.config().active_profile_id.is_some());

        mgr.deactivate_profile().await.unwrap();
        assert!(mgr.config().active_profile_id.is_none());
    }

    #[tokio::test]
    async fn test_update_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        mgr.update_profile(
            &id,
            Some(2_000_000),
            Some(500_000),
            Some(5),
            Some("Updated"),
        )
        .await
        .unwrap();

        let profile = mgr.get_profile(&id).unwrap();
        assert_eq!(profile.speed_limit_bps, 2_000_000);
        assert_eq!(profile.upload_limit_bps, 500_000);
        assert_eq!(profile.max_concurrent, 5);
        assert_eq!(profile.description.as_deref(), Some("Updated"));
    }

    #[tokio::test]
    async fn test_update_partial() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr
            .create_profile("Test", 1000, Some("Original"))
            .await
            .unwrap();
        // Only update speed limit
        mgr.update_profile(&id, Some(5_000_000), None, None, None)
            .await
            .unwrap();

        let profile = mgr.get_profile(&id).unwrap();
        assert_eq!(profile.speed_limit_bps, 5_000_000);
        assert_eq!(profile.upload_limit_bps, 0); // unchanged
        assert_eq!(profile.description.as_deref(), Some("Original")); // unchanged
    }

    #[tokio::test]
    async fn test_list_profiles_sorted() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        mgr.create_profile("Zebra", 100, None).await.unwrap();
        mgr.create_profile("Alpha", 200, None).await.unwrap();
        mgr.create_profile("Middle", 300, None).await.unwrap();

        let list = mgr.list_profiles();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].name, "Alpha");
        assert_eq!(list[1].name, "Middle");
        assert_eq!(list[2].name, "Zebra");
    }

    #[tokio::test]
    async fn test_summary() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        mgr.activate_profile(&id).await.unwrap();

        let summary = mgr.summary();
        assert_eq!(summary.total_profiles, 1);
        assert!(summary.active_profile.is_some());
        assert_eq!(summary.active_profile.unwrap().id, id);
        assert_eq!(summary.profiles.len(), 1);
        assert!(summary.profiles[0].is_active);
    }

    #[tokio::test]
    async fn test_summary_no_active() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        mgr.create_profile("Test", 1000, None).await.unwrap();

        let summary = mgr.summary();
        assert_eq!(summary.total_profiles, 1);
        assert!(summary.active_profile.is_none());
        assert!(!summary.profiles[0].is_active);
    }

    #[tokio::test]
    async fn test_persistence() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr
            .create_profile("Night", 0, Some("Unlimited night speed"))
            .await
            .unwrap();
        mgr.activate_profile(&id).await.unwrap();

        // Create a new manager pointing to the same directory
        let mut mgr2 = SpeedProfileManager::new(tmp.path());
        mgr2.load().await.unwrap();

        assert_eq!(mgr2.config().profiles.len(), 1);
        assert_eq!(mgr2.config().profiles[0].name, "Night");
        assert_eq!(mgr2.config().profiles[0].speed_limit_bps, 0);
        assert_eq!(
            mgr2.config().active_profile_id.as_deref(),
            Some(id.as_str())
        );
    }

    #[tokio::test]
    async fn test_persistence_missing_file() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        // Should succeed with defaults when no file exists
        mgr.load().await.unwrap();
        assert!(mgr.config().profiles.is_empty());
    }

    #[tokio::test]
    async fn test_max_profiles_limit() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());
        mgr.config.max_profiles = 2;

        mgr.create_profile("One", 100, None).await.unwrap();
        mgr.create_profile("Two", 200, None).await.unwrap();

        let result = mgr.create_profile("Three", 300, None).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SpeedProfileError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_activate_increments_use_count() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        assert_eq!(mgr.get_profile(&id).unwrap().use_count, 0);

        mgr.activate_profile(&id).await.unwrap();
        assert_eq!(mgr.get_profile(&id).unwrap().use_count, 1);

        mgr.activate_profile(&id).await.unwrap();
        assert_eq!(mgr.get_profile(&id).unwrap().use_count, 2);
    }

    #[tokio::test]
    async fn test_active_profile_accessor() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        assert!(mgr.active_profile().is_none());

        let id = mgr.create_profile("Test", 1000, None).await.unwrap();
        mgr.activate_profile(&id).await.unwrap();

        let active = mgr.active_profile().unwrap();
        assert_eq!(active.id, id);
        assert_eq!(active.speed_limit_bps, 1000);
    }

    // ============================================================================
    // Phase 205: Comprehensive Test Coverage
    // ============================================================================

    // --- Serialization Tests ---

    #[test]
    fn test_speed_profile_serde_roundtrip() {
        let profile = SpeedProfile::new("test_id".to_string(), "Test Name".to_string(), 1_000_000);
        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: SpeedProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "test_id");
        assert_eq!(deserialized.name, "Test Name");
        assert_eq!(deserialized.speed_limit_bps, 1_000_000);
        assert_eq!(deserialized.upload_limit_bps, 0);
        assert_eq!(deserialized.max_concurrent, 0);
        assert_eq!(deserialized.use_count, 0);
        assert!(deserialized.last_used_at.is_none());
        assert!(deserialized.description.is_none());
    }

    #[test]
    fn test_speed_profile_serde_with_optional_fields() {
        let mut profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 500_000);
        profile.description = Some("Test description".to_string());
        profile.upload_limit_bps = 250_000;
        profile.max_concurrent = 5;
        profile.use_count = 42;
        profile.last_used_at = Some(Local::now());

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: SpeedProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.description.as_deref(),
            Some("Test description")
        );
        assert_eq!(deserialized.upload_limit_bps, 250_000);
        assert_eq!(deserialized.max_concurrent, 5);
        assert_eq!(deserialized.use_count, 42);
        assert!(deserialized.last_used_at.is_some());
    }

    #[test]
    fn test_speed_profile_serde_extra_fields_ignored() {
        let json = r#"{
            "id": "test",
            "name": "Test",
            "description": null,
            "speed_limit_bps": 1000,
            "upload_limit_bps": 0,
            "max_concurrent": 0,
            "created_at": "2024-01-01T00:00:00+00:00",
            "last_used_at": null,
            "use_count": 0,
            "extra_unknown_field": "should be ignored"
        }"#;
        let profile: SpeedProfile = serde_json::from_str(json).unwrap();
        assert_eq!(profile.id, "test");
        assert_eq!(profile.speed_limit_bps, 1000);
    }

    #[test]
    fn test_speed_profiles_config_serde_roundtrip() {
        let mut config = SpeedProfilesConfig::default();
        let profile = SpeedProfile::new("p1".to_string(), "Profile 1".to_string(), 1_000_000);
        config.profiles.push(profile);
        config.active_profile_id = Some("p1".to_string());
        config.max_profiles = 100;

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SpeedProfilesConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.profiles.len(), 1);
        assert_eq!(deserialized.active_profile_id.as_deref(), Some("p1"));
        assert_eq!(deserialized.max_profiles, 100);
    }

    #[test]
    fn test_speed_profiles_config_default_values() {
        let config = SpeedProfilesConfig::default();
        assert!(config.profiles.is_empty());
        assert!(config.active_profile_id.is_none());
        assert_eq!(config.max_profiles, 50);
    }

    #[test]
    fn test_speed_profiles_config_serde_extra_fields_ignored() {
        let json = r#"{
            "profiles": [],
            "active_profile_id": null,
            "max_profiles": 50,
            "unknown_field": 123
        }"#;
        let config: SpeedProfilesConfig = serde_json::from_str(json).unwrap();
        assert!(config.profiles.is_empty());
        assert_eq!(config.max_profiles, 50);
    }

    #[test]
    fn test_speed_profiles_summary_serde_roundtrip() {
        let summary = SpeedProfilesSummary {
            total_profiles: 3,
            active_profile: None,
            profiles: vec![],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedProfilesSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_profiles, 3);
        assert!(deserialized.active_profile.is_none());
    }

    #[test]
    fn test_speed_profile_info_serde_roundtrip() {
        let info = SpeedProfileInfo {
            id: "test_id".to_string(),
            name: "Test Name".to_string(),
            description: Some("Description".to_string()),
            speed_limit_bps: 1_000_000,
            upload_limit_bps: 500_000,
            max_concurrent: 10,
            is_active: true,
            use_count: 5,
            last_used_at: None,
            created_at: Local::now(),
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: SpeedProfileInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "test_id");
        assert_eq!(deserialized.name, "Test Name");
        assert!(deserialized.is_active);
        assert_eq!(deserialized.use_count, 5);
    }

    // --- Error Display Tests ---

    #[test]
    fn test_speed_profile_error_not_found_display() {
        let err = SpeedProfileError::NotFound("missing_profile".to_string());
        assert_eq!(err.to_string(), "profile not found: missing_profile");
    }

    #[test]
    fn test_speed_profile_error_already_exists_display() {
        let err = SpeedProfileError::AlreadyExists("duplicate_name".to_string());
        assert_eq!(
            err.to_string(),
            "profile name already exists: duplicate_name"
        );
    }

    #[test]
    fn test_speed_profile_error_invalid_name_display() {
        let err = SpeedProfileError::InvalidName("bad name".to_string());
        assert_eq!(err.to_string(), "invalid profile name: bad name");
    }

    #[test]
    fn test_speed_profile_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err = SpeedProfileError::Io(io_err);
        assert!(err.to_string().contains("permission denied"));
    }

    #[test]
    fn test_speed_profile_error_serialize_display() {
        let json_err = serde_json::from_str::<SpeedProfilesConfig>("invalid json").unwrap_err();
        let err = SpeedProfileError::Serialize(json_err);
        assert!(
            err.to_string().contains("serialization error") || err.to_string().contains("JSON")
        );
    }

    #[test]
    fn test_speed_profile_error_debug() {
        let err = SpeedProfileError::NotFound("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("NotFound"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_speed_profile_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: SpeedProfileError = io_err.into();
        assert!(matches!(err, SpeedProfileError::Io(_)));
    }

    #[test]
    fn test_speed_profile_error_from_serde() {
        let json_err = serde_json::from_str::<SpeedProfilesConfig>("{}").unwrap_err();
        // This should succeed, so let's use invalid JSON
        let json_err = serde_json::from_str::<SpeedProfilesConfig>("{invalid}").unwrap_err();
        let err: SpeedProfileError = json_err.into();
        assert!(matches!(err, SpeedProfileError::Serialize(_)));
    }

    // --- Struct Traits Tests ---

    #[test]
    fn test_speed_profile_clone() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 1000);
        let cloned = profile.clone();
        assert_eq!(cloned.id, profile.id);
        assert_eq!(cloned.name, profile.name);
        assert_eq!(cloned.speed_limit_bps, profile.speed_limit_bps);
    }

    #[test]
    fn test_speed_profile_debug() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 1000);
        let debug_str = format!("{:?}", profile);
        assert!(debug_str.contains("SpeedProfile"));
        assert!(debug_str.contains("id"));
    }

    #[test]
    fn test_speed_profiles_config_clone() {
        let config = SpeedProfilesConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.max_profiles, config.max_profiles);
        assert_eq!(cloned.profiles.len(), config.profiles.len());
    }

    #[test]
    fn test_speed_profiles_config_debug() {
        let config = SpeedProfilesConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SpeedProfilesConfig"));
    }

    #[test]
    fn test_speed_profiles_summary_clone() {
        let summary = SpeedProfilesSummary {
            total_profiles: 5,
            active_profile: None,
            profiles: vec![],
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_profiles, 5);
    }

    #[test]
    fn test_speed_profile_info_clone() {
        let info = SpeedProfileInfo {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: None,
            speed_limit_bps: 1000,
            upload_limit_bps: 0,
            max_concurrent: 0,
            is_active: false,
            use_count: 0,
            last_used_at: None,
            created_at: Local::now(),
        };
        let cloned = info.clone();
        assert_eq!(cloned.id, "id");
        assert_eq!(cloned.speed_limit_bps, 1000);
    }

    #[test]
    fn test_speed_profile_manager_debug() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedProfileManager::new(tmp.path());
        let debug_str = format!("{:?}", mgr);
        assert!(debug_str.contains("SpeedProfileManager"));
    }

    // --- format_speed_bps Edge Cases ---

    #[test]
    fn test_format_speed_bps_zero() {
        assert_eq!(format_speed_bps(0), "Unlimited");
    }

    #[test]
    fn test_format_speed_bps_small_values() {
        assert_eq!(format_speed_bps(1), "1 B/s");
        assert_eq!(format_speed_bps(999), "999 B/s");
    }

    #[test]
    fn test_format_speed_bps_exact_kb() {
        assert_eq!(format_speed_bps(1_000), "1.0 KB/s");
        assert_eq!(format_speed_bps(10_000), "10.0 KB/s");
        assert_eq!(format_speed_bps(999_999), "1000.0 KB/s");
    }

    #[test]
    fn test_format_speed_bps_exact_mb() {
        assert_eq!(format_speed_bps(1_000_000), "1.0 MB/s");
        assert_eq!(format_speed_bps(10_000_000), "10.0 MB/s");
        assert_eq!(format_speed_bps(100_000_000), "100.0 MB/s");
    }

    #[test]
    fn test_format_speed_bps_large_values() {
        assert_eq!(format_speed_bps(1_000_000_000), "1000.0 MB/s");
        assert_eq!(
            format_speed_bps(u64::MAX),
            format!("{:.1} MB/s", u64::MAX as f64 / 1_000_000.0)
        );
    }

    // --- parse_speed_bps Edge Cases ---

    #[test]
    fn test_parse_speed_bps_case_insensitive() {
        assert_eq!(parse_speed_bps("1MB/S").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("1mb/s").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("1Mb/S").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("UNLIMITED").unwrap(), 0);
        assert_eq!(parse_speed_bps("None").unwrap(), 0);
    }

    #[test]
    fn test_parse_speed_bps_with_whitespace() {
        assert_eq!(parse_speed_bps("  1MB/s  ").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("  512KB/s  ").unwrap(), 512_000);
        assert_eq!(parse_speed_bps("   ").unwrap(), 0);
    }

    #[test]
    fn test_parse_speed_bps_mbps_suffix() {
        assert_eq!(parse_speed_bps("1mbps").unwrap(), 1_000_000);
        assert_eq!(parse_speed_bps("10Mbps").unwrap(), 10_000_000);
    }

    #[test]
    fn test_parse_speed_bps_kbps_suffix() {
        assert_eq!(parse_speed_bps("512kbps").unwrap(), 512_000);
        assert_eq!(parse_speed_bps("100Kbps").unwrap(), 100_000);
    }

    #[test]
    fn test_parse_speed_bps_bps_suffix() {
        assert_eq!(parse_speed_bps("100bps").unwrap(), 100);
        assert_eq!(parse_speed_bps("500Bps").unwrap(), 500);
    }

    #[test]
    fn test_parse_speed_bps_decimal_values() {
        assert_eq!(parse_speed_bps("1.5MB/s").unwrap(), 1_500_000);
        assert_eq!(parse_speed_bps("0.5KB/s").unwrap(), 500);
        assert_eq!(parse_speed_bps("2.5m").unwrap(), 2_500_000);
    }

    #[test]
    fn test_parse_speed_bps_negative_error() {
        let result = parse_speed_bps("-1MB/s");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SpeedProfileError::InvalidName(_)));
    }

    #[test]
    fn test_parse_speed_bps_invalid_string_error() {
        let result = parse_speed_bps("not a speed");
        assert!(result.is_err());
    }

    // --- profile_id_from_name Edge Cases ---

    #[test]
    fn test_profile_id_from_name_unicode() {
        let id = profile_id_from_name("测试配置");
        // Chinese characters are alphanumeric in Rust, so they are preserved
        assert_eq!(id, "测试配置");
    }

    #[test]
    fn test_profile_id_from_name_special_chars() {
        assert_eq!(profile_id_from_name("Test@#$%"), "test");
        assert_eq!(profile_id_from_name("!!!@@@"), "");
        assert_eq!(profile_id_from_name("a__b__c"), "a__b__c");
    }

    #[test]
    fn test_profile_id_from_name_empty() {
        assert_eq!(profile_id_from_name(""), "");
        assert_eq!(profile_id_from_name("   "), "");
    }

    #[test]
    fn test_profile_id_from_name_numbers() {
        assert_eq!(profile_id_from_name("Profile 123"), "profile_123");
        assert_eq!(profile_id_from_name("100KB/s"), "100kb_s");
    }

    // --- Manager Edge Cases ---

    #[tokio::test]
    async fn test_manager_new_creates_correct_path() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedProfileManager::new(tmp.path());
        assert!(mgr.config_path.ends_with("speed_profiles.json"));
    }

    #[tokio::test]
    async fn test_get_profile_not_found() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedProfileManager::new(tmp.path());
        assert!(mgr.get_profile("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_list_profiles_empty() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedProfileManager::new(tmp.path());
        let list = mgr.list_profiles();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_list_profiles_multiple() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        mgr.create_profile("C Profile", 300, None).await.unwrap();
        mgr.create_profile("A Profile", 100, None).await.unwrap();
        mgr.create_profile("B Profile", 200, None).await.unwrap();

        let list = mgr.list_profiles();
        assert_eq!(list.len(), 3);
        // Should be sorted by name
        assert_eq!(list[0].name, "A Profile");
        assert_eq!(list[1].name, "B Profile");
        assert_eq!(list[2].name, "C Profile");
    }

    #[tokio::test]
    async fn test_update_profile_not_found() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let result = mgr
            .update_profile("nonexistent", Some(1000), None, None, None)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(SpeedProfileError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_update_profile_clear_description() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr
            .create_profile("Test", 1000, Some("Original description"))
            .await
            .unwrap();
        // Update with new description
        mgr.update_profile(&id, None, None, None, Some("New description"))
            .await
            .unwrap();

        let profile = mgr.get_profile(&id).unwrap();
        assert_eq!(profile.description.as_deref(), Some("New description"));
    }

    #[tokio::test]
    async fn test_deactivate_when_no_active_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        // Should succeed even when no profile is active
        mgr.deactivate_profile().await.unwrap();
        assert!(mgr.config().active_profile_id.is_none());
    }

    #[tokio::test]
    async fn test_activate_switches_active_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id1 = mgr.create_profile("Profile 1", 1000, None).await.unwrap();
        let id2 = mgr.create_profile("Profile 2", 2000, None).await.unwrap();

        mgr.activate_profile(&id1).await.unwrap();
        assert_eq!(
            mgr.config().active_profile_id.as_deref(),
            Some(id1.as_str())
        );

        // Activate second profile should switch
        mgr.activate_profile(&id2).await.unwrap();
        assert_eq!(
            mgr.config().active_profile_id.as_deref(),
            Some(id2.as_str())
        );
    }

    #[tokio::test]
    async fn test_delete_non_active_profile_preserves_active() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id1 = mgr.create_profile("Profile 1", 1000, None).await.unwrap();
        let id2 = mgr.create_profile("Profile 2", 2000, None).await.unwrap();

        mgr.activate_profile(&id1).await.unwrap();
        mgr.delete_profile(&id2).await.unwrap();

        // Active profile should still be id1
        assert_eq!(
            mgr.config().active_profile_id.as_deref(),
            Some(id1.as_str())
        );
    }

    #[tokio::test]
    async fn test_max_profiles_zero_unlimited() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());
        mgr.config.max_profiles = 0; // 0 means unlimited

        // Should be able to create many profiles
        for i in 0..100 {
            mgr.create_profile(&format!("Profile {}", i), 1000, None)
                .await
                .unwrap();
        }
        assert_eq!(mgr.config().profiles.len(), 100);
    }

    // --- Persistence Edge Cases ---

    #[tokio::test]
    async fn test_persistence_corrupt_json() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("speed_profiles.json");

        // Write corrupt JSON
        tokio::fs::write(&config_path, "not valid json {{{")
            .await
            .unwrap();

        let mut mgr = SpeedProfileManager::new(tmp.path());
        let result = mgr.load().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_persistence_empty_file() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("speed_profiles.json");

        // Write empty file
        tokio::fs::write(&config_path, "").await.unwrap();

        let mut mgr = SpeedProfileManager::new(tmp.path());
        let result = mgr.load().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_persistence_atomic_write() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        mgr.create_profile("Test", 1000, None).await.unwrap();

        // Verify no temp file remains
        let temp_path = tmp.path().join("speed_profiles.json.tmp");
        assert!(!temp_path.exists());

        // Verify main file exists
        let main_path = tmp.path().join("speed_profiles.json");
        assert!(main_path.exists());
    }

    #[tokio::test]
    async fn test_persistence_overwrite() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        mgr.create_profile("First", 1000, None).await.unwrap();
        mgr.create_profile("Second", 2000, None).await.unwrap();

        // Load and verify both profiles
        let mut mgr2 = SpeedProfileManager::new(tmp.path());
        mgr2.load().await.unwrap();
        assert_eq!(mgr2.config().profiles.len(), 2);

        // Delete one and save
        mgr2.delete_profile("first").await.unwrap();

        // Load again and verify
        let mut mgr3 = SpeedProfileManager::new(tmp.path());
        mgr3.load().await.unwrap();
        assert_eq!(mgr3.config().profiles.len(), 1);
        assert_eq!(mgr3.config().profiles[0].name, "Second");
    }

    #[tokio::test]
    async fn test_persistence_with_active_profile() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr
            .create_profile("Active Profile", 5_000_000, Some("This is active"))
            .await
            .unwrap();
        mgr.activate_profile(&id).await.unwrap();

        let mut mgr2 = SpeedProfileManager::new(tmp.path());
        mgr2.load().await.unwrap();

        assert_eq!(
            mgr2.config().active_profile_id.as_deref(),
            Some(id.as_str())
        );
        let active = mgr2.active_profile().unwrap();
        assert_eq!(active.name, "Active Profile");
        assert_eq!(active.speed_limit_bps, 5_000_000);
    }

    #[tokio::test]
    async fn test_persistence_unicode_names() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        // Unicode names get converted to IDs with underscores
        mgr.create_profile("Profile 配置", 1000, Some("描述"))
            .await
            .unwrap();

        let mut mgr2 = SpeedProfileManager::new(tmp.path());
        mgr2.load().await.unwrap();

        assert_eq!(mgr2.config().profiles.len(), 1);
        assert_eq!(mgr2.config().profiles[0].name, "Profile 配置");
        assert_eq!(
            mgr2.config().profiles[0].description.as_deref(),
            Some("描述")
        );
    }

    // --- Summary Edge Cases ---

    #[tokio::test]
    async fn test_summary_multiple_profiles() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id1 = mgr.create_profile("Profile 1", 1000, None).await.unwrap();
        let _id2 = mgr.create_profile("Profile 2", 2000, None).await.unwrap();
        let _id3 = mgr.create_profile("Profile 3", 3000, None).await.unwrap();

        mgr.activate_profile(&id1).await.unwrap();

        let summary = mgr.summary();
        assert_eq!(summary.total_profiles, 3);
        assert!(summary.active_profile.is_some());
        assert_eq!(summary.active_profile.as_ref().unwrap().id, id1);
        assert_eq!(summary.profiles.len(), 3);

        // Only one should be active
        let active_count = summary.profiles.iter().filter(|p| p.is_active).count();
        assert_eq!(active_count, 1);
    }

    #[tokio::test]
    async fn test_summary_profile_info_fields() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        let id = mgr
            .create_profile("Test", 1_500_000, Some("Description"))
            .await
            .unwrap();
        mgr.activate_profile(&id).await.unwrap();

        let summary = mgr.summary();
        let info = &summary.profiles[0];

        assert_eq!(info.id, id);
        assert_eq!(info.name, "Test");
        assert_eq!(info.description.as_deref(), Some("Description"));
        assert_eq!(info.speed_limit_bps, 1_500_000);
        assert_eq!(info.upload_limit_bps, 0);
        assert_eq!(info.max_concurrent, 0);
        assert!(info.is_active);
        assert_eq!(info.use_count, 1);
        assert!(info.last_used_at.is_some());
    }

    // --- Complete Workflow Test ---

    #[tokio::test]
    async fn test_complete_workflow() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedProfileManager::new(tmp.path());

        // Create profiles
        let night_id = mgr
            .create_profile("Night Unlimited", 0, Some("No limit at night"))
            .await
            .unwrap();
        let work_id = mgr
            .create_profile("Work Hours", 1_000_000, Some("Limited during work"))
            .await
            .unwrap();
        let game_id = mgr
            .create_profile("Gaming", 500_000, Some("Low bandwidth for gaming"))
            .await
            .unwrap();

        assert_eq!(mgr.config().profiles.len(), 3);

        // Activate night profile
        let (speed, _, _) = mgr.activate_profile(&night_id).await.unwrap();
        assert_eq!(speed, 0); // Unlimited

        // Verify active
        let active = mgr.active_profile().unwrap();
        assert_eq!(active.id, night_id);

        // Switch to work profile
        let (speed, _, _) = mgr.activate_profile(&work_id).await.unwrap();
        assert_eq!(speed, 1_000_000);

        // Verify use counts
        assert_eq!(mgr.get_profile(&night_id).unwrap().use_count, 1);
        assert_eq!(mgr.get_profile(&work_id).unwrap().use_count, 1);

        // Update work profile
        mgr.update_profile(
            &work_id,
            Some(2_000_000),
            None,
            None,
            Some("Updated work limit"),
        )
        .await
        .unwrap();
        assert_eq!(
            mgr.get_profile(&work_id).unwrap().speed_limit_bps,
            2_000_000
        );

        // Delete gaming profile
        mgr.delete_profile(&game_id).await.unwrap();
        assert_eq!(mgr.config().profiles.len(), 2);

        // Verify summary
        let summary = mgr.summary();
        assert_eq!(summary.total_profiles, 2);
        assert!(summary.active_profile.is_some());
        assert_eq!(summary.active_profile.unwrap().id, work_id);

        // Persist and reload
        let mut mgr2 = SpeedProfileManager::new(tmp.path());
        mgr2.load().await.unwrap();
        assert_eq!(mgr2.config().profiles.len(), 2);
        assert_eq!(
            mgr2.config().active_profile_id.as_deref(),
            Some(work_id.as_str())
        );

        // Deactivate
        mgr2.deactivate_profile().await.unwrap();
        assert!(mgr2.active_profile().is_none());
    }

    // --- to_info Tests ---

    #[test]
    fn test_to_info_active_true() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 1000);
        let info = profile.to_info(true);
        assert!(info.is_active);
    }

    #[test]
    fn test_to_info_active_false() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 1000);
        let info = profile.to_info(false);
        assert!(!info.is_active);
    }

    #[test]
    fn test_to_info_preserves_all_fields() {
        let mut profile = SpeedProfile::new("my_id".to_string(), "My Name".to_string(), 5_000_000);
        profile.description = Some("My description".to_string());
        profile.upload_limit_bps = 2_500_000;
        profile.max_concurrent = 10;
        profile.use_count = 42;

        let info = profile.to_info(false);
        assert_eq!(info.id, "my_id");
        assert_eq!(info.name, "My Name");
        assert_eq!(info.description.as_deref(), Some("My description"));
        assert_eq!(info.speed_limit_bps, 5_000_000);
        assert_eq!(info.upload_limit_bps, 2_500_000);
        assert_eq!(info.max_concurrent, 10);
        assert_eq!(info.use_count, 42);
    }

    // --- format_speed_limit Tests ---

    #[test]
    fn test_format_speed_limit_unlimited() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 0);
        assert_eq!(profile.format_speed_limit(), "Unlimited");
    }

    #[test]
    fn test_format_speed_limit_kb() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 512_000);
        assert_eq!(profile.format_speed_limit(), "512.0 KB/s");
    }

    #[test]
    fn test_format_speed_limit_mb() {
        let profile = SpeedProfile::new("id".to_string(), "Name".to_string(), 10_000_000);
        assert_eq!(profile.format_speed_limit(), "10.0 MB/s");
    }
}
