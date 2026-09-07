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
    use tempfile::tempdir;

    // === SpeedBoostConfig defaults ===

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
    fn test_default_config_has_night_preset() {
        let config = SpeedBoostConfig::default();
        let night = config.presets.get("night").unwrap();
        assert_eq!(night.name, "Night Boost");
        assert_eq!(night.multiplier, 2.0);
        assert_eq!(night.duration_secs, 3600);
    }

    #[test]
    fn test_default_config_has_turbo_preset() {
        let config = SpeedBoostConfig::default();
        let turbo = config.presets.get("turbo").unwrap();
        assert_eq!(turbo.name, "Turbo Mode");
        assert_eq!(turbo.multiplier, 5.0);
        assert_eq!(turbo.duration_secs, 1800);
    }

    #[test]
    fn test_default_config_has_unlimited_preset() {
        let config = SpeedBoostConfig::default();
        let unlimited = config.presets.get("unlimited").unwrap();
        assert_eq!(unlimited.name, "Unlimited");
        assert_eq!(unlimited.multiplier, 100.0);
        assert_eq!(unlimited.duration_secs, 900);
    }

    // === SpeedBoostConfig serde ===

    #[test]
    fn test_config_serde_roundtrip() {
        let config = SpeedBoostConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedBoostConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.default_duration_secs, config.default_duration_secs);
        assert_eq!(loaded.default_multiplier, config.default_multiplier);
        assert_eq!(loaded.max_duration_secs, config.max_duration_secs);
        assert_eq!(loaded.max_multiplier, config.max_multiplier);
        assert_eq!(loaded.presets.len(), config.presets.len());
    }

    #[test]
    fn test_config_pretty_serde() {
        let config = SpeedBoostConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let loaded: SpeedBoostConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"default_duration_secs":100,"default_multiplier":1.5,"max_duration_secs":200,"max_multiplier":10.0,"presets":{},"scheduled_windows":[],"extra_field":"ignored"}"#;
        let config: SpeedBoostConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.default_duration_secs, 100);
    }

    // === SpeedBoostConfig Clone/Debug ===

    #[test]
    fn test_config_clone() {
        let config = SpeedBoostConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.default_duration_secs, config.default_duration_secs);
    }

    #[test]
    fn test_config_clone_independence() {
        let mut config = SpeedBoostConfig::default();
        let cloned = config.clone();
        config.enabled = false;
        assert!(cloned.enabled);
    }

    #[test]
    fn test_config_debug() {
        let config = SpeedBoostConfig::default();
        let debug = format!("{config:?}");
        assert!(debug.contains("SpeedBoostConfig"));
        assert!(debug.contains("enabled"));
    }

    // === BoostPreset ===

    #[test]
    fn test_boost_preset_clone() {
        let preset = BoostPreset {
            name: "Test".to_string(),
            multiplier: 3.0,
            duration_secs: 900,
            description: "Desc".to_string(),
        };
        let cloned = preset.clone();
        assert_eq!(cloned.name, "Test");
        assert_eq!(cloned.multiplier, 3.0);
        assert_eq!(cloned.duration_secs, 900);
        assert_eq!(cloned.description, "Desc");
    }

    #[test]
    fn test_boost_preset_clone_independence() {
        let mut preset = BoostPreset {
            name: "Test".to_string(),
            multiplier: 3.0,
            duration_secs: 900,
            description: "Desc".to_string(),
        };
        let cloned = preset.clone();
        preset.name = "Changed".to_string();
        assert_eq!(cloned.name, "Test");
    }

    #[test]
    fn test_boost_preset_debug() {
        let preset = BoostPreset {
            name: "Turbo".to_string(),
            multiplier: 5.0,
            duration_secs: 1800,
            description: "Fast".to_string(),
        };
        let debug = format!("{preset:?}");
        assert!(debug.contains("Turbo"));
        assert!(debug.contains("5.0"));
    }

    #[test]
    fn test_boost_preset_serde_roundtrip() {
        let preset = BoostPreset {
            name: "Night".to_string(),
            multiplier: 2.0,
            duration_secs: 3600,
            description: "Nighttime boost".to_string(),
        };
        let json = serde_json::to_string(&preset).unwrap();
        let loaded: BoostPreset = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.name, preset.name);
        assert_eq!(loaded.multiplier, preset.multiplier);
        assert_eq!(loaded.duration_secs, preset.duration_secs);
        assert_eq!(loaded.description, preset.description);
    }

    #[test]
    fn test_boost_preset_unicode() {
        let preset = BoostPreset {
            name: "夜间加速".to_string(),
            multiplier: 2.0,
            duration_secs: 3600,
            description: "🚀 快速下载".to_string(),
        };
        let json = serde_json::to_string(&preset).unwrap();
        let loaded: BoostPreset = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.name, "夜间加速");
        assert_eq!(loaded.description, "🚀 快速下载");
    }

    // === ScheduledBoostWindow ===

    #[test]
    fn test_scheduled_window_clone() {
        let window = ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "Window 1".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![0, 6],
            enabled: true,
        };
        let cloned = window.clone();
        assert_eq!(cloned.id, "w1");
        assert_eq!(cloned.days_of_week, vec![0, 6]);
    }

    #[test]
    fn test_scheduled_window_clone_independence() {
        let mut window = ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "Window 1".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![0, 6],
            enabled: true,
        };
        let cloned = window.clone();
        window.enabled = false;
        assert!(cloned.enabled);
    }

    #[test]
    fn test_scheduled_window_debug() {
        let window = ScheduledBoostWindow {
            id: "night".to_string(),
            name: "Night".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![],
            enabled: true,
        };
        let debug = format!("{window:?}");
        assert!(debug.contains("night"));
        assert!(debug.contains("22:00"));
    }

    #[test]
    fn test_scheduled_window_serde_roundtrip() {
        let window = ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "Window 1".to_string(),
            multiplier: 3.0,
            start_time: "08:00".to_string(),
            end_time: "12:00".to_string(),
            days_of_week: vec![1, 2, 3, 4, 5],
            enabled: true,
        };
        let json = serde_json::to_string(&window).unwrap();
        let loaded: ScheduledBoostWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.id, window.id);
        assert_eq!(loaded.multiplier, window.multiplier);
        assert_eq!(loaded.days_of_week, window.days_of_week);
    }

    // === ActiveBoost ===

    #[test]
    fn test_active_boost_clone() {
        let boost = ActiveBoost {
            started_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(300),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        let cloned = boost.clone();
        assert_eq!(cloned.multiplier, boost.multiplier);
        assert_eq!(cloned.source, boost.source);
    }

    #[test]
    fn test_active_boost_debug() {
        let boost = ActiveBoost {
            started_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(300),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        let debug = format!("{boost:?}");
        assert!(debug.contains("ActiveBoost"));
        assert!(debug.contains("manual"));
    }

    #[test]
    fn test_active_boost_serde_roundtrip() {
        let now = Utc::now();
        let boost = ActiveBoost {
            started_at: now,
            expires_at: now + chrono::Duration::seconds(300),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        let json = serde_json::to_string(&boost).unwrap();
        let loaded: ActiveBoost = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.multiplier, boost.multiplier);
        assert_eq!(loaded.source, boost.source);
        assert_eq!(loaded.original_limit, boost.original_limit);
    }

    #[test]
    fn test_active_boost_remaining_secs_future() {
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
    fn test_active_boost_remaining_secs_expired() {
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
    fn test_active_boost_is_expired_false() {
        let boost = ActiveBoost {
            started_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(600),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        assert!(!boost.is_expired());
    }

    // === BoostStartResult variants ===

    #[test]
    fn test_boost_start_result_started() {
        let boost = ActiveBoost {
            started_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(300),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        let result = BoostStartResult::Started(boost);
        assert!(matches!(result, BoostStartResult::Started(_)));
    }

    #[test]
    fn test_boost_start_result_disabled() {
        let result = BoostStartResult::Disabled;
        assert!(matches!(result, BoostStartResult::Disabled));
    }

    #[test]
    fn test_boost_start_result_invalid_params() {
        let result = BoostStartResult::InvalidParams("bad".to_string());
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_boost_start_result_already_active() {
        let result = BoostStartResult::AlreadyActive;
        assert!(matches!(result, BoostStartResult::AlreadyActive));
    }

    #[test]
    fn test_boost_start_result_clone() {
        let result = BoostStartResult::Disabled;
        let cloned = result.clone();
        assert!(matches!(cloned, BoostStartResult::Disabled));
    }

    #[test]
    fn test_boost_start_result_debug() {
        let result = BoostStartResult::Disabled;
        let debug = format!("{result:?}");
        assert!(debug.contains("Disabled"));
    }

    // === SpeedBoostStatus ===

    #[test]
    fn test_status_serde_roundtrip() {
        let status = SpeedBoostStatus {
            active_boost: None,
            total_boosts_started: 10,
            total_boosts_completed: 8,
            total_manual_boosts: 6,
            total_scheduled_boosts: 4,
            preset_count: 3,
            scheduled_window_count: 2,
        };
        let json = serde_json::to_string(&status).unwrap();
        let loaded: SpeedBoostStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_boosts_started, 10);
        assert_eq!(loaded.total_boosts_completed, 8);
        assert_eq!(loaded.total_manual_boosts, 6);
        assert_eq!(loaded.total_scheduled_boosts, 4);
    }

    #[test]
    fn test_status_clone() {
        let status = SpeedBoostStatus {
            active_boost: None,
            total_boosts_started: 5,
            total_boosts_completed: 3,
            total_manual_boosts: 2,
            total_scheduled_boosts: 1,
            preset_count: 3,
            scheduled_window_count: 1,
        };
        let cloned = status.clone();
        assert_eq!(cloned.total_boosts_started, 5);
        assert_eq!(cloned.total_scheduled_boosts, 1);
    }

    #[test]
    fn test_status_debug() {
        let status = SpeedBoostStatus {
            active_boost: None,
            total_boosts_started: 0,
            total_boosts_completed: 0,
            total_manual_boosts: 0,
            total_scheduled_boosts: 0,
            preset_count: 3,
            scheduled_window_count: 0,
        };
        let debug = format!("{status:?}");
        assert!(debug.contains("SpeedBoostStatus"));
    }

    // === SpeedBoostManager::new / default ===

    #[test]
    fn test_manager_new() {
        let manager = SpeedBoostManager::new();
        assert!(manager.config().enabled);
        assert_eq!(manager.total_started, 0);
        assert_eq!(manager.total_completed, 0);
        assert_eq!(manager.total_manual, 0);
        assert_eq!(manager.total_scheduled, 0);
        assert!(manager.active_boost.is_none());
    }

    #[test]
    fn test_manager_default_equals_new() {
        let new = SpeedBoostManager::new();
        let default = SpeedBoostManager::default();
        assert_eq!(default.config().enabled, new.config().enabled);
        assert_eq!(
            default.config().default_duration_secs,
            new.config().default_duration_secs
        );
        assert_eq!(default.config().presets.len(), new.config().presets.len());
    }

    #[test]
    fn test_manager_with_config() {
        let config = SpeedBoostConfig {
            enabled: false,
            default_duration_secs: 600,
            default_multiplier: 3.0,
            max_duration_secs: 7200,
            max_multiplier: 50.0,
            presets: HashMap::new(),
            scheduled_windows: Vec::new(),
        };
        let manager = SpeedBoostManager::with_config(config);
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().default_duration_secs, 600);
        assert_eq!(manager.config().default_multiplier, 3.0);
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = SpeedBoostManager::new();
        assert!(manager.config().enabled);
        let mut new_config = manager.config().clone();
        new_config.enabled = false;
        manager.set_config(new_config);
        assert!(!manager.config().enabled);
    }

    // === start_boost ===

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
    fn test_start_boost_invalid_duration_zero() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(0), Some(2.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_invalid_duration_too_large() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(999999), Some(2.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_invalid_multiplier_too_low() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(0.5));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_invalid_multiplier_too_high() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(200.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_multiplier_exactly_one() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(1.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_multiplier_just_above_one() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(1.01));
        assert!(matches!(result, BoostStartResult::Started(_)));
    }

    #[test]
    fn test_start_boost_multiplier_at_max() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(100.0));
        assert!(matches!(result, BoostStartResult::Started(_)));
    }

    #[test]
    fn test_start_boost_multiplier_just_above_max() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(100.01));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_duration_at_max() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(14400), Some(2.0));
        assert!(matches!(result, BoostStartResult::Started(_)));
    }

    #[test]
    fn test_start_boost_duration_just_over_max() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(14401), Some(2.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_boost_duration_at_one() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(1), Some(2.0));
        assert!(matches!(result, BoostStartResult::Started(_)));
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
    fn test_start_boost_zero_limit() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(0), Some(600), Some(2.0));
        match result {
            BoostStartResult::Started(boost) => {
                assert_eq!(boost.boosted_limit, None);
            }
            _ => panic!("Expected Started result"),
        }
    }

    #[test]
    fn test_start_boost_default_params() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), None, None);
        match result {
            BoostStartResult::Started(boost) => {
                assert_eq!(boost.multiplier, 2.0);
            }
            _ => panic!("Expected Started result"),
        }
    }

    #[test]
    fn test_start_boost_boosted_limit_calculation() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(500_000), Some(600), Some(3.0));
        match result {
            BoostStartResult::Started(boost) => {
                assert_eq!(boost.boosted_limit, Some(1_500_000));
            }
            _ => panic!("Expected Started result"),
        }
    }

    #[test]
    fn test_start_boost_source_is_manual() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        match result {
            BoostStartResult::Started(boost) => {
                assert_eq!(boost.source, "manual");
            }
            _ => panic!("Expected Started result"),
        }
    }

    #[test]
    fn test_start_boost_increments_counters() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert_eq!(manager.total_started, 1);
        assert_eq!(manager.total_manual, 1);
        assert_eq!(manager.total_scheduled, 0);
        manager.active_boost = None;
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert_eq!(manager.total_started, 2);
        assert_eq!(manager.total_manual, 2);
    }

    // === stop_boost ===

    #[test]
    fn test_stop_boost_active() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert!(manager.stop_boost());
        assert!(manager.active_boost.is_none());
    }

    #[test]
    fn test_stop_boost_none() {
        let mut manager = SpeedBoostManager::new();
        assert!(!manager.stop_boost());
    }

    #[test]
    fn test_stop_boost_idempotent() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert!(manager.stop_boost());
        assert!(!manager.stop_boost());
    }

    // === status ===

    #[test]
    fn test_status_with_active_boost() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        let status = manager.status();
        assert!(status.active_boost.is_some());
        assert_eq!(status.total_boosts_started, 1);
        assert_eq!(status.total_manual_boosts, 1);
        assert_eq!(status.preset_count, 3);
    }

    #[test]
    fn test_status_no_active_boost() {
        let manager = SpeedBoostManager::new();
        let status = manager.status();
        assert!(status.active_boost.is_none());
        assert_eq!(status.total_boosts_started, 0);
    }

    #[test]
    fn test_status_hides_expired_boost() {
        let mut manager = SpeedBoostManager::new();
        let boost = ActiveBoost {
            started_at: Utc::now() - chrono::Duration::seconds(400),
            expires_at: Utc::now() - chrono::Duration::seconds(1),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        manager.active_boost = Some(boost);
        let status = manager.status();
        assert!(status.active_boost.is_none());
    }

    #[test]
    fn test_status_scheduled_window_count() {
        let mut manager = SpeedBoostManager::new();
        manager.add_scheduled_window(ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "W1".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![],
            enabled: true,
        });
        let status = manager.status();
        assert_eq!(status.scheduled_window_count, 1);
    }

    // === effective_limit ===

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
    fn test_effective_limit_none_base() {
        let manager = SpeedBoostManager::new();
        assert_eq!(manager.effective_limit(None), None);
    }

    #[test]
    fn test_effective_limit_with_expired_boost() {
        let mut manager = SpeedBoostManager::new();
        let boost = ActiveBoost {
            started_at: Utc::now() - chrono::Duration::seconds(400),
            expires_at: Utc::now() - chrono::Duration::seconds(1),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        manager.active_boost = Some(boost);
        assert_eq!(manager.effective_limit(Some(500_000)), Some(500_000));
    }

    // === process_expired ===

    #[test]
    fn test_process_expired_removes_expired() {
        let mut manager = SpeedBoostManager::new();
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
    fn test_process_expired_keeps_active() {
        let mut manager = SpeedBoostManager::new();
        let boost = ActiveBoost {
            started_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(300),
            multiplier: 2.0,
            original_limit: Some(1_000_000),
            boosted_limit: Some(2_000_000),
            source: "manual".to_string(),
        };
        manager.active_boost = Some(boost);
        manager.process_expired();
        assert!(manager.active_boost.is_some());
        assert_eq!(manager.total_completed, 0);
    }

    #[test]
    fn test_process_expired_no_boost() {
        let mut manager = SpeedBoostManager::new();
        manager.process_expired();
        assert!(manager.active_boost.is_none());
        assert_eq!(manager.total_completed, 0);
    }

    #[test]
    fn test_process_expired_idempotent() {
        let mut manager = SpeedBoostManager::new();
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
        manager.process_expired();
        assert_eq!(manager.total_completed, 1);
    }

    // === Preset operations ===

    #[test]
    fn test_add_preset() {
        let mut manager = SpeedBoostManager::new();
        let preset = BoostPreset {
            name: "Custom".to_string(),
            multiplier: 3.0,
            duration_secs: 900,
            description: "Custom preset".to_string(),
        };
        assert!(manager.add_preset("custom", preset));
        assert_eq!(manager.list_presets().len(), 4);
        assert!(manager.list_presets().contains_key("custom"));
    }

    #[test]
    fn test_add_preset_replaces_existing() {
        let mut manager = SpeedBoostManager::new();
        let preset1 = BoostPreset {
            name: "V1".to_string(),
            multiplier: 2.0,
            duration_secs: 600,
            description: "V1".to_string(),
        };
        let preset2 = BoostPreset {
            name: "V2".to_string(),
            multiplier: 4.0,
            duration_secs: 1200,
            description: "V2".to_string(),
        };
        manager.add_preset("custom", preset1);
        manager.add_preset("custom", preset2);
        assert_eq!(manager.list_presets().get("custom").unwrap().name, "V2");
    }

    #[test]
    fn test_remove_preset_existing() {
        let mut manager = SpeedBoostManager::new();
        assert!(manager.remove_preset("night"));
        assert_eq!(manager.list_presets().len(), 2);
    }

    #[test]
    fn test_remove_preset_nonexistent() {
        let mut manager = SpeedBoostManager::new();
        assert!(!manager.remove_preset("nonexistent"));
        assert_eq!(manager.list_presets().len(), 3);
    }

    #[test]
    fn test_list_presets_default_count() {
        let manager = SpeedBoostManager::new();
        assert_eq!(manager.list_presets().len(), 3);
    }

    // === start_preset_boost ===

    #[test]
    fn test_start_preset_boost_success() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_preset_boost("night", Some(1_000_000));
        assert!(matches!(result, BoostStartResult::Started(_)));
        // Internal active_boost has the preset source
        assert_eq!(
            manager.active_boost.as_ref().unwrap().source,
            "preset:night"
        );
        assert_eq!(manager.active_boost.as_ref().unwrap().multiplier, 2.0);
    }

    #[test]
    fn test_start_preset_boost_turbo() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_preset_boost("turbo", Some(1_000_000));
        assert!(matches!(result, BoostStartResult::Started(_)));
        assert_eq!(
            manager.active_boost.as_ref().unwrap().source,
            "preset:turbo"
        );
        assert_eq!(manager.active_boost.as_ref().unwrap().multiplier, 5.0);
    }

    #[test]
    fn test_start_preset_boost_not_found() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_preset_boost("nonexistent", Some(1_000_000));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_preset_boost_with_none_limit() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_preset_boost("night", None);
        assert!(matches!(result, BoostStartResult::Started(_)));
    }

    // === Scheduled window operations ===

    #[test]
    fn test_add_scheduled_window() {
        let mut manager = SpeedBoostManager::new();
        let window = ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "Window 1".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![0, 6],
            enabled: true,
        };
        assert!(manager.add_scheduled_window(window));
        assert_eq!(manager.list_scheduled_windows().len(), 1);
    }

    #[test]
    fn test_add_multiple_scheduled_windows() {
        let mut manager = SpeedBoostManager::new();
        for i in 0..5 {
            manager.add_scheduled_window(ScheduledBoostWindow {
                id: format!("w{i}"),
                name: format!("Window {i}"),
                multiplier: 2.0,
                start_time: "22:00".to_string(),
                end_time: "06:00".to_string(),
                days_of_week: vec![],
                enabled: true,
            });
        }
        assert_eq!(manager.list_scheduled_windows().len(), 5);
    }

    #[test]
    fn test_remove_scheduled_window_existing() {
        let mut manager = SpeedBoostManager::new();
        manager.add_scheduled_window(ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "W1".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![],
            enabled: true,
        });
        assert!(manager.remove_scheduled_window("w1"));
        assert!(manager.list_scheduled_windows().is_empty());
    }

    #[test]
    fn test_remove_scheduled_window_nonexistent() {
        let mut manager = SpeedBoostManager::new();
        assert!(!manager.remove_scheduled_window("nonexistent"));
    }

    #[test]
    fn test_set_scheduled_window_enabled() {
        let mut manager = SpeedBoostManager::new();
        manager.add_scheduled_window(ScheduledBoostWindow {
            id: "w1".to_string(),
            name: "W1".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![],
            enabled: true,
        });
        assert!(manager.set_scheduled_window_enabled("w1", false));
        assert!(!manager.list_scheduled_windows()[0].enabled);
        assert!(manager.set_scheduled_window_enabled("w1", true));
        assert!(manager.list_scheduled_windows()[0].enabled);
    }

    #[test]
    fn test_set_scheduled_window_enabled_nonexistent() {
        let mut manager = SpeedBoostManager::new();
        assert!(!manager.set_scheduled_window_enabled("nonexistent", false));
    }

    #[test]
    fn test_list_scheduled_windows_empty() {
        let manager = SpeedBoostManager::new();
        assert!(manager.list_scheduled_windows().is_empty());
    }

    // === calculate_window_duration ===

    #[test]
    fn test_calculate_window_duration_full_hour() {
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
        assert_eq!(manager.calculate_window_duration(&window, "22:00"), 3600);
    }

    #[test]
    fn test_calculate_window_duration_half_hour() {
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
        assert_eq!(manager.calculate_window_duration(&window, "22:30"), 1800);
    }

    #[test]
    fn test_calculate_window_duration_at_end() {
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
        assert_eq!(manager.calculate_window_duration(&window, "23:00"), 0);
    }

    #[test]
    fn test_calculate_window_duration_past_end() {
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
        assert_eq!(manager.calculate_window_duration(&window, "23:30"), 0);
    }

    #[test]
    fn test_calculate_window_duration_invalid_end_format() {
        let manager = SpeedBoostManager::new();
        let window = ScheduledBoostWindow {
            id: "test".to_string(),
            name: "Test".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "invalid".to_string(),
            days_of_week: vec![],
            enabled: true,
        };
        assert_eq!(
            manager.calculate_window_duration(&window, "22:30"),
            manager.config().default_duration_secs
        );
    }

    #[test]
    fn test_calculate_window_duration_invalid_current_time() {
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
        assert_eq!(
            manager.calculate_window_duration(&window, "bad"),
            manager.config().default_duration_secs
        );
    }

    #[test]
    fn test_calculate_window_duration_clamped_to_max() {
        let config = SpeedBoostConfig {
            max_duration_secs: 1800,
            ..Default::default()
        };
        let manager = SpeedBoostManager::with_config(config);
        let window = ScheduledBoostWindow {
            id: "test".to_string(),
            name: "Test".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "23:30".to_string(),
            days_of_week: vec![],
            enabled: true,
        };
        assert_eq!(manager.calculate_window_duration(&window, "22:00"), 1800);
    }

    // === Persistence ===

    #[test]
    fn test_save_load_config() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("speed_boost.json");
        let manager = SpeedBoostManager::new();
        assert!(manager.save_config(&path).is_ok());
        let loaded = SpeedBoostManager::load_config(&path).unwrap();
        assert_eq!(loaded.enabled, manager.config().enabled);
        assert_eq!(loaded.presets.len(), manager.config().presets.len());
    }

    #[test]
    fn test_save_config_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("speed_boost.json");
        let manager = SpeedBoostManager::new();
        manager.save_config(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_save_config_overwrite() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("speed_boost.json");
        let manager = SpeedBoostManager::new();
        manager.save_config(&path).unwrap();
        let mut config2 = manager.config().clone();
        config2.enabled = false;
        let manager2 = SpeedBoostManager::with_config(config2);
        manager2.save_config(&path).unwrap();
        let loaded = SpeedBoostManager::load_config(&path).unwrap();
        assert!(!loaded.enabled);
    }

    #[test]
    fn test_load_config_not_found() {
        let result = SpeedBoostManager::load_config(std::path::Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_corrupted_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("speed_boost.json");
        std::fs::write(&path, "not json").unwrap();
        let result = SpeedBoostManager::load_config(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_save_load_config_unicode() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("speed_boost.json");
        let mut config = SpeedBoostConfig::default();
        config.presets.insert(
            "夜间".to_string(),
            BoostPreset {
                name: "🚀 加速".to_string(),
                multiplier: 2.0,
                duration_secs: 3600,
                description: "夜间加速下载".to_string(),
            },
        );
        let manager = SpeedBoostManager::with_config(config);
        manager.save_config(&path).unwrap();
        let loaded = SpeedBoostManager::load_config(&path).unwrap();
        let preset = loaded.presets.get("夜间").unwrap();
        assert_eq!(preset.name, "🚀 加速");
    }

    // === Complex workflows ===

    #[test]
    fn test_complete_lifecycle() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert!(matches!(result, BoostStartResult::Started(_)));
        assert_eq!(manager.effective_limit(Some(1_000_000)), Some(2_000_000));
        let status = manager.status();
        assert!(status.active_boost.is_some());
        assert_eq!(status.total_boosts_started, 1);
        assert!(manager.stop_boost());
        assert_eq!(manager.effective_limit(Some(1_000_000)), Some(1_000_000));
    }

    #[test]
    fn test_preset_lifecycle() {
        let mut manager = SpeedBoostManager::new();
        manager.add_preset(
            "custom",
            BoostPreset {
                name: "Custom".to_string(),
                multiplier: 3.0,
                duration_secs: 900,
                description: "Custom boost".to_string(),
            },
        );
        let result = manager.start_preset_boost("custom", Some(1_000_000));
        assert!(matches!(result, BoostStartResult::Started(_)));
        assert_eq!(
            manager.active_boost.as_ref().unwrap().source,
            "preset:custom"
        );
        assert!(manager.remove_preset("custom"));
        assert!(manager.stop_boost());
    }

    #[test]
    fn test_multiple_managers_independent() {
        let mut m1 = SpeedBoostManager::new();
        let mut m2 = SpeedBoostManager::new();
        let _ = m1.start_boost(Some(1_000_000), Some(600), Some(2.0));
        assert!(m2.active_boost.is_none());
        assert_eq!(m2.total_started, 0);
        let _ = m2.start_boost(Some(2_000_000), Some(300), Some(3.0));
        assert_eq!(m1.total_started, 1);
        assert_eq!(m2.total_started, 1);
    }

    #[test]
    fn test_boost_then_restart_after_stop() {
        let mut manager = SpeedBoostManager::new();
        let _ = manager.start_boost(Some(1_000_000), Some(600), Some(2.0));
        manager.stop_boost();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(3.0));
        assert!(matches!(result, BoostStartResult::Started(_)));
        assert_eq!(manager.total_started, 2);
    }

    #[test]
    fn test_scheduled_window_with_unicode() {
        let mut manager = SpeedBoostManager::new();
        manager.add_scheduled_window(ScheduledBoostWindow {
            id: "夜间窗口".to_string(),
            name: "🌙 夜间".to_string(),
            multiplier: 2.0,
            start_time: "22:00".to_string(),
            end_time: "06:00".to_string(),
            days_of_week: vec![1, 2, 3, 4, 5],
            enabled: true,
        });
        assert_eq!(manager.list_scheduled_windows().len(), 1);
        assert_eq!(manager.list_scheduled_windows()[0].name, "🌙 夜间");
    }

    #[test]
    fn test_config_custom_values() {
        let config = SpeedBoostConfig {
            enabled: false,
            default_duration_secs: 300,
            default_multiplier: 1.5,
            max_duration_secs: 3600,
            max_multiplier: 10.0,
            presets: HashMap::new(),
            scheduled_windows: Vec::new(),
        };
        let manager = SpeedBoostManager::with_config(config);
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().default_duration_secs, 300);
        assert_eq!(manager.config().max_multiplier, 10.0);
    }

    #[test]
    fn test_start_boost_with_max_multiplier() {
        let config = SpeedBoostConfig {
            max_multiplier: 10.0,
            ..Default::default()
        };
        let mut manager = SpeedBoostManager::with_config(config);
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(10.0));
        assert!(matches!(result, BoostStartResult::Started(_)));
    }

    #[test]
    fn test_start_boost_negative_multiplier() {
        let mut manager = SpeedBoostManager::new();
        let result = manager.start_boost(Some(1_000_000), Some(600), Some(-1.0));
        assert!(matches!(result, BoostStartResult::InvalidParams(_)));
    }
}
