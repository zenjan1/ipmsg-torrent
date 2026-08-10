//! Download Speed Boost Mode
//!
//! System-wide temporary speed boost that temporarily increases the global download
//! speed limit. Distinct from per-task speed_burst, this affects all downloads globally.
//! Supports scheduled boost windows, named presets, and automatic expiration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for speed boost feature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedBoostConfig {
    /// Whether speed boost feature is enabled
    pub enabled: bool,
    /// Default boost duration in seconds (if not specified)
    pub default_duration_secs: u64,
    /// Default speed multiplier during boost (e.g., 2.0 = double speed)
    pub default_multiplier: f64,
    /// Maximum allowed boost duration in seconds
    pub max_duration_secs: u64,
    /// Maximum allowed multiplier
    pub max_multiplier: f64,
    /// Named boost presets
    pub presets: HashMap<String, BoostPreset>,
    /// Scheduled boost windows
    pub scheduled_windows: Vec<ScheduledBoostWindow>,
}

impl Default for SpeedBoostConfig {
    fn default() -> Self {
        let mut presets = HashMap::new();
        presets.insert(
            "night".to_string(),
            BoostPreset {
                name: "Night Boost".to_string(),
                multiplier: 2.0,
                duration_secs: 3600,
                description: "2x speed for 1 hour (nighttime)".to_string(),
            },
        );
        presets.insert(
            "turbo".to_string(),
            BoostPreset {
                name: "Turbo Mode".to_string(),
                multiplier: 5.0,
                duration_secs: 1800,
                description: "5x speed for 30 minutes".to_string(),
            },
        );
        presets.insert(
            "unlimited".to_string(),
            BoostPreset {
                name: "Unlimited".to_string(),
                multiplier: 100.0,
                duration_secs: 900,
                description: "Effectively unlimited for 15 minutes".to_string(),
            },
        );

        Self {
            enabled: true,
            default_duration_secs: 1800, // 30 minutes
            default_multiplier: 2.0,
            max_duration_secs: 14400, // 4 hours max
            max_multiplier: 100.0,
            presets,
            scheduled_windows: Vec::new(),
        }
    }
}

/// Named boost preset for quick activation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoostPreset {
    /// Display name
    pub name: String,
    /// Speed multiplier
    pub multiplier: f64,
    /// Duration in seconds
    pub duration_secs: u64,
    /// Description
    pub description: String,
}

/// Scheduled boost window for automatic activation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledBoostWindow {
    /// Unique identifier
    pub id: String,
    /// Display name
    pub name: String,
    /// Speed multiplier during window
    pub multiplier: f64,
    /// Start time (HH:MM format, 24-hour)
    pub start_time: String,
    /// End time (HH:MM format, 24-hour)
    pub end_time: String,
    /// Days of week when active (0=Sunday, 1=Monday, ..., 6=Saturday)
    pub days_of_week: Vec<u8>,
    /// Whether this window is enabled
    pub enabled: bool,
}

/// State of an active speed boost
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveBoost {
    /// When the boost started
    pub started_at: DateTime<Utc>,
    /// When the boost expires
    pub expires_at: DateTime<Utc>,
    /// Speed multiplier applied
    pub multiplier: f64,
    /// Original global speed limit before boost (None = unlimited)
    pub original_limit: Option<u64>,
    /// Boosted speed limit in bytes/sec
    pub boosted_limit: Option<u64>,
    /// Source of boost (preset name, scheduled window id, or "manual")
    pub source: String,
}

impl ActiveBoost {
    /// Check if this boost has expired
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    /// Get remaining duration in seconds
    pub fn remaining_secs(&self) -> u64 {
        let now = Utc::now();
        if now >= self.expires_at {
            0
        } else {
            (self.expires_at - now).num_seconds().max(0) as u64
        }
    }
}

/// Result of attempting to start a boost
#[derive(Debug, Clone)]
pub enum BoostStartResult {
    /// Boost started successfully
    Started(ActiveBoost),
    /// Feature is disabled
    Disabled,
    /// Invalid parameters
    InvalidParams(String),
    /// Another boost is already active
    AlreadyActive,
}

/// Status of the speed boost system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedBoostStatus {
    /// Currently active boost (if any)
    pub active_boost: Option<ActiveBoost>,
    /// Total boosts started since tracking began
    pub total_boosts_started: u64,
    /// Total boosts completed (expired naturally)
    pub total_boosts_completed: u64,
    /// Total manual boosts triggered
    pub total_manual_boosts: u64,
    /// Total scheduled boosts triggered
    pub total_scheduled_boosts: u64,
    /// Named presets available
    pub preset_count: usize,
    /// Scheduled windows configured
    pub scheduled_window_count: usize,
}

/// Manager for speed boost operations
#[derive(Debug)]
pub struct SpeedBoostManager {
    /// Configuration
    config: SpeedBoostConfig,
    /// Currently active boost
    active_boost: Option<ActiveBoost>,
    /// Total boosts started
    total_started: u64,
    /// Total boosts completed (expired naturally)
    total_completed: u64,
    /// Total manual boosts
    total_manual: u64,
    /// Total scheduled boosts
    total_scheduled: u64,
}

impl SpeedBoostManager {
    /// Create a new manager with default configuration
    pub fn new() -> Self {
        Self {
            config: SpeedBoostConfig::default(),
            active_boost: None,
            total_started: 0,
            total_completed: 0,
            total_manual: 0,
            total_scheduled: 0,
        }
    }

    /// Create a new manager with custom configuration
    pub fn with_config(config: SpeedBoostConfig) -> Self {
        Self {
            config,
            active_boost: None,
            total_started: 0,
            total_completed: 0,
            total_manual: 0,
            total_scheduled: 0,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &SpeedBoostConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: SpeedBoostConfig) {
        self.config = config;
    }

    /// Start a manual speed boost
    ///
    /// # Arguments
    /// * `current_limit` - Current global speed limit (None = unlimited)
    /// * `duration_secs` - Optional custom duration (uses default if None)
    /// * `multiplier` - Optional custom multiplier (uses default if None)
    pub fn start_boost(
        &mut self,
        current_limit: Option<u64>,
        duration_secs: Option<u64>,
        multiplier: Option<f64>,
    ) -> BoostStartResult {
        // Check if feature is enabled
        if !self.config.enabled {
            return BoostStartResult::Disabled;
        }

        // Check if another boost is already active
        if self.active_boost.as_ref().is_some_and(|b| !b.is_expired()) {
            return BoostStartResult::AlreadyActive;
        }

        // Validate parameters
        let duration = duration_secs.unwrap_or(self.config.default_duration_secs);
        let mult = multiplier.unwrap_or(self.config.default_multiplier);

        if duration == 0 || duration > self.config.max_duration_secs {
            return BoostStartResult::InvalidParams(format!(
                "Duration must be between 1 and {} seconds",
                self.config.max_duration_secs
            ));
        }

        if mult <= 1.0 || mult > self.config.max_multiplier {
            return BoostStartResult::InvalidParams(format!(
                "Multiplier must be between 1.0 and {}",
                self.config.max_multiplier
            ));
        }

        // Calculate boosted limit
        let boosted_limit = match current_limit {
            Some(limit) if limit > 0 => Some((limit as f64 * mult) as u64),
            _ => None, // Unlimited stays unlimited
        };

        let now = Utc::now();
        let boost = ActiveBoost {
            started_at: now,
            expires_at: now + chrono::Duration::seconds(duration as i64),
            multiplier: mult,
            original_limit: current_limit,
            boosted_limit,
            source: "manual".to_string(),
        };

        self.active_boost = Some(boost.clone());
        self.total_started += 1;
        self.total_manual += 1;

        BoostStartResult::Started(boost)
    }

    /// Start a boost using a named preset
    pub fn start_preset_boost(
        &mut self,
        preset_name: &str,
        current_limit: Option<u64>,
    ) -> BoostStartResult {
        let preset = match self.config.presets.get(preset_name) {
            Some(p) => p.clone(),
            None => {
                return BoostStartResult::InvalidParams(format!(
                    "Preset '{}' not found",
                    preset_name
                ));
            }
        };

        let result = self.start_boost(
            current_limit,
            Some(preset.duration_secs),
            Some(preset.multiplier),
        );

        // Update source if started successfully
        if let Some(ref mut boost) = self.active_boost {
            boost.source = format!("preset:{}", preset_name);
        }

        result
    }

    /// Check and activate scheduled boost windows
    ///
    /// Should be called periodically (e.g., every minute) to check if any
    /// scheduled window should be activated.
    pub fn check_scheduled_windows(&mut self, current_limit: Option<u64>) -> Option<ActiveBoost> {
        if !self.config.enabled {
            return None;
        }

        // Check if a boost is already active
        if self.active_boost.as_ref().is_some_and(|b| !b.is_expired()) {
            return None;
        }

        let now = Utc::now();
        let current_time = now.format("%H:%M").to_string();
        let current_day = now.format("%w").to_string().parse::<u8>().unwrap_or(0);

        // Find matching window first (avoid borrow conflict)
        let matching = self
            .config
            .scheduled_windows
            .iter()
            .find(|window| {
                if !window.enabled {
                    return false;
                }
                if !window.days_of_week.is_empty() && !window.days_of_week.contains(&current_day) {
                    return false;
                }
                current_time >= window.start_time && current_time < window.end_time
            })
            .cloned();

        if let Some(window) = matching {
            let duration_secs = self.calculate_window_duration(&window, &current_time);
            if let BoostStartResult::Started(boost) =
                self.start_boost(current_limit, Some(duration_secs), Some(window.multiplier))
            {
                if let Some(ref mut active) = self.active_boost {
                    active.source = format!("scheduled:{}", window.id);
                }
                self.total_scheduled += 1;
                return Some(boost);
            }
        }

        None
    }

    /// Calculate remaining duration for a scheduled window
    fn calculate_window_duration(&self, window: &ScheduledBoostWindow, current_time: &str) -> u64 {
        // Parse end time
        let end_parts: Vec<&str> = window.end_time.split(':').collect();
        if end_parts.len() != 2 {
            return self.config.default_duration_secs;
        }

        let end_hour = end_parts[0].parse::<i64>().unwrap_or(0);
        let end_min = end_parts[1].parse::<i64>().unwrap_or(0);
        let end_total_min = end_hour * 60 + end_min;

        // Parse current time
        let current_parts: Vec<&str> = current_time.split(':').collect();
        if current_parts.len() != 2 {
            return self.config.default_duration_secs;
        }

        let current_hour = current_parts[0].parse::<i64>().unwrap_or(0);
        let current_min = current_parts[1].parse::<i64>().unwrap_or(0);
        let current_total_min = current_hour * 60 + current_min;

        // Calculate remaining minutes
        let remaining_min = (end_total_min - current_total_min).max(0);
        let remaining_secs = (remaining_min * 60) as u64;

        // Clamp to max duration
        remaining_secs.min(self.config.max_duration_secs)
    }

    /// Stop the active boost
    pub fn stop_boost(&mut self) -> bool {
        self.active_boost.take().is_some()
    }

    /// Get current status
    pub fn status(&self) -> SpeedBoostStatus {
        // Clean up expired boost for reporting
        let active = self.active_boost.as_ref().and_then(|b| {
            if b.is_expired() {
                None
            } else {
                Some(b.clone())
            }
        });

        SpeedBoostStatus {
            active_boost: active,
            total_boosts_started: self.total_started,
            total_boosts_completed: self.total_completed,
            total_manual_boosts: self.total_manual,
            total_scheduled_boosts: self.total_scheduled,
            preset_count: self.config.presets.len(),
            scheduled_window_count: self.config.scheduled_windows.len(),
        }
    }

    /// Get the current effective speed limit
    ///
    /// Returns the boosted limit if a boost is active, otherwise returns the original limit.
    pub fn effective_limit(&self, base_limit: Option<u64>) -> Option<u64> {
        if let Some(ref boost) = self.active_boost
            && !boost.is_expired()
        {
            return boost.boosted_limit.or(base_limit);
        }
        base_limit
    }

    /// Process expired boosts (call periodically)
    pub fn process_expired(&mut self) {
        if let Some(ref boost) = self.active_boost
            && boost.is_expired()
        {
            self.active_boost = None;
            self.total_completed += 1;
        }
    }

    /// Add a named preset
    pub fn add_preset(&mut self, id: &str, preset: BoostPreset) -> bool {
        self.config.presets.insert(id.to_string(), preset);
        true
    }

    /// Remove a named preset
    pub fn remove_preset(&mut self, id: &str) -> bool {
        self.config.presets.remove(id).is_some()
    }

    /// List all presets
    pub fn list_presets(&self) -> &HashMap<String, BoostPreset> {
        &self.config.presets
    }

    /// Add a scheduled window
    pub fn add_scheduled_window(&mut self, window: ScheduledBoostWindow) -> bool {
        self.config.scheduled_windows.push(window);
        true
    }

    /// Remove a scheduled window
    pub fn remove_scheduled_window(&mut self, id: &str) -> bool {
        let initial_len = self.config.scheduled_windows.len();
        self.config.scheduled_windows.retain(|w| w.id != id);
        self.config.scheduled_windows.len() < initial_len
    }

    /// List all scheduled windows
    pub fn list_scheduled_windows(&self) -> &[ScheduledBoostWindow] {
        &self.config.scheduled_windows
    }

    /// Enable or disable a scheduled window
    pub fn set_scheduled_window_enabled(&mut self, id: &str, enabled: bool) -> bool {
        if let Some(window) = self
            .config
            .scheduled_windows
            .iter_mut()
            .find(|w| w.id == id)
        {
            window.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Save configuration to file
    pub fn save_config(&self, path: &std::path::Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    /// Load configuration from file
    pub fn load_config(path: &std::path::Path) -> std::io::Result<SpeedBoostConfig> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl Default for SpeedBoostManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SpeedBoostConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_duration_secs, 1800);
        assert_eq!(config.default_multiplier, 2.0);
        assert_eq!(config.max_duration_secs, 14400);
        assert_eq!(config.max_multiplier, 100.0);
        assert_eq!(config.presets.len(), 3);
        assert!(config.scheduled_windows.is_empty());
    }

    #[test]
    fn test_start_boost_success() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));

        match result {
            BoostStartResult::Started(boost) => {
                assert_eq!(boost.multiplier, 2.0);
                assert_eq!(boost.original_limit, Some(1_000_000));
                assert_eq!(boost.boosted_limit, Some(2_000_000));
                assert_eq!(boost.source, "manual");
                assert!(!boost.is_expired());
            }
            _ => panic!("Expected Started result"),
        }

        assert_eq!(manager.total_started, 1);
        assert_eq!(manager.total_manual, 1);
    }

    #[test]
    fn test_start_boost_disabled() {
        let mut manager = SpeedBoostManager::new();
        manager.config.enabled = false;

        let result = manager.start_boost(Some(1_000_000), None, None);
        assert!(matches!(result, BoostStartResult::Disabled));
    }

    #[test]
    fn test_start_boost_already_active() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));

        let result = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert!(matches!(result, BoostStartResult::AlreadyActive));
    }

    #[test]
    fn test_start_boost_invalid_duration() {
        let mut manager = SpeedBoostManager::new();

        let result = manager.start_boost(Some(1_000_000), Some(0), Some(2.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));

        let result = manager.start_boost(Some(1_000_000), Some(999999), Some(2.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_invalid_multiplier() {
        let mut manager = SpeedBoostManager::new();

        let result = manager.start_boost(Some(1_000_000), Some(600), Some(0.5));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));

        let result = manager.start_boost(Some(1_000_000), Some(600), Some(200.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_unlimited() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(None, Some(600), Some(2.0));

        match result {
            BoostStartResult::Started(boost) => {
                assert_eq!(boost.original_limit, None);
                assert_eq!(boost.boosted_limit, None);
            }
            _ => panic!("Expected Started result"),
        }
    }

    #[test]
    fn test_stop_boost() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));

        assert!(manager.stop_boost());
        assert!(manager.active_boost.is_none());
        assert!(!manager.stop_boost()); // Already stopped
    }

    #[test]
    fn test_status() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));

        let status = manager.status();
        assert!(status.active_boost.is_some());
        assert_eq!(status.total_boosts_started, 1);
        assert_eq!(status.total_manual_boosts, 1);
        assert_eq!(status.preset_count, 3);
    }

    #[test]
    fn test_effective_limit_with_boost() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));

        assert_eq!(manager.effective_limit(Some(1_000_000)), Some(2_000_000));
    }

    #[test]
    fn test_effective_limit_without_boost() {
        let manager = SpeedBoostManager::new();
        assert_eq!(manager.effective_limit(Some(1_000_000)), Some(1_000_000));
    }

    #[test]
    fn test_process_expired() {
        let mut manager = SpeedBoostManager::new();

        // Create a boost that's already expired
        let boost = ActiveBoost {
            started_at: Utc::now() - chrono::Duration::seconds(100),
            expires_at: Utc::now() - chrono::Duration::seconds(1),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        manager.active_boost = Some(boost);

        manager.process_expired();
        assert!(manager.active_boost.is_none());
        assert_eq!(manager.total_completed, 1);
    }

    #[test]
    fn test_preset_operations() {
        let mut manager = SpeedBoostManager::new();

        // Add preset
        let preset = BoostPreset {
            name: "Test Preset".to_string(),
            multiplier: 3.0,
            duration_secs: 900,
            description: "Test".to_string(),
        };
        assert!(manager.add_preset("test", preset));
        assert_eq!(manager.list_presets().len(), 4); // 3 default + 1 new

        // Start preset boost
        let result = manager.start_preset_boost("test", Some(1_000_000));
        assert!(matches!(result, BoostStartResult::Started(_)));

        // Remove preset
        assert!(manager.remove_preset("test"));
        assert_eq!(manager.list_presets().len(), 3);

        // Remove non-existent
        assert!(!manager.remove_preset("nonexistent"));
    }

    #[test]
    fn test_start_preset_boost_not_found() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_preset_boost("nonexistent", Some(1_000_000));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_scheduled_window_operations() {
        let mut manager = SpeedBoostManager::new();

        let window = ScheduledBoostWindow {
            id: "night_window".to_string(),
            name: "Night Window".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![0, 6], // Weekend
            enabled: true,
        };

        assert!(manager.add_scheduled_window(window));
        assert_eq!(manager.list_scheduled_windows().len(), 1);

        // Enable/disable
        assert!(manager.set_scheduled_window_enabled("night_window", false));
        assert!(!manager.list_scheduled_windows()[0].enabled);

        // Remove
        assert!(manager.remove_scheduled_window("night_window"));
        assert!(manager.list_scheduled_windows().is_empty());
    }

    #[test]
    fn test_save_load_config() {
        let manager = SpeedBoostManager::new();
        let temp_path = std::env::temp_dir().join("speed_boost_test.json");

        // Save
        assert!(manager.save_config(&temp_path).is_ok());

        // Load
        let loaded = SpeedBoostManager::load_config(&temp_path);
        assert!(loaded.is_ok());

        let loaded_config = loaded.unwrap();
        assert_eq!(loaded_config.enabled, manager.config.enabled);
        assert_eq!(loaded_config.presets.len(), manager.config.presets.len());

        // Cleanup
        let _ = std::fs::remove_file(&temp_path);
    }

    #[test]
    fn test_load_config_not_found() {
        let result = SpeedBoostManager::load_config(std::path::Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_active_boost_remaining_secs() {
        let boost = ActiveBoost {
            started_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(300),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };

        let remaining = boost.remaining_secs();
        assert!(remaining > 290 && remaining <= 300);
    }

    #[test]
    fn test_active_boost_expired() {
        let boost = ActiveBoost {
            started_at: Utc::now() - chrono::Duration::seconds(400),
            expires_at: Utc::now() - chrono::Duration::seconds(100),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };

        assert!(boost.is_expired());
        assert_eq!(boost.remaining_secs(), 0);
    }

    #[test]
    fn test_calculate_window_duration() {
        let manager = SpeedBoostManager::new();
        let window = ScheduledBoostWindow {
            id: "test".to_string(),
            name: "Test".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "23:00".to_string(),
            days_of_week: vec![],
            enabled: true,
        };

        // At 22:30, should have 30 minutes remaining
        let duration = manager.calculate_window_duration(&window, "22:30");
        assert_eq!(duration, 1800); // 30 minutes in seconds

        // At 22:00, should have 60 minutes remaining
        let duration = manager.calculate_window_duration(&window, "22:00");
        assert_eq!(duration, 3600); // 60 minutes in seconds
    }
}
