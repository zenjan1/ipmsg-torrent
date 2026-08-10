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
}
