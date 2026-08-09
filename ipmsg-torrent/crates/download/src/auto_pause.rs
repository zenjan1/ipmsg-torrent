//! Auto-Pause Scheduler for Download Tasks
//!
//! Automatically pause downloads during configurable peak hours and resume them
//! during off-peak times. This complements bandwidth scheduling (which throttles)
//! by fully pausing/resuming tasks based on time-of-day rules.
//!
//! Features:
//! - Configurable peak hours (e.g., 9am-5pm on weekdays)
//! - Auto-pause running downloads when peak hours start
//! - Auto-resume previously paused downloads when peak hours end
//! - Persist configuration to disk (auto_pause_config.json)
//! - REST API endpoints for management
//! - CLI commands for control
//!
//! Use cases:
//! - Avoid using bandwidth during work hours
//! - Resume downloads automatically during off-peak times
//! - Prevent downloads from interfering with video calls or gaming

use chrono::{DateTime, Datelike, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use tokio::fs;

/// Errors from auto-pause operations
#[derive(Error, Debug)]
pub enum AutoPauseError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid time format: {0}")]
    InvalidTime(String),
}

/// Time window for peak hours
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeakHours {
    /// Start hour (0-23)
    pub start_hour: u8,
    /// Start minute (0-59)
    pub start_minute: u8,
    /// End hour (0-23)
    pub end_hour: u8,
    /// End minute (0-59)
    pub end_minute: u8,
    /// Days of week when this applies (None = all days)
    pub days: Option<Vec<Weekday>>,
}

impl PeakHours {
    /// Create a new peak hours window
    pub fn new(
        start_hour: u8,
        start_minute: u8,
        end_hour: u8,
        end_minute: u8,
        days: Option<Vec<Weekday>>,
    ) -> Result<Self, AutoPauseError> {
        if start_hour > 23 || end_hour > 23 {
            return Err(AutoPauseError::InvalidTime("Hour must be 0-23".to_string()));
        }
        if start_minute > 59 || end_minute > 59 {
            return Err(AutoPauseError::InvalidTime(
                "Minute must be 0-59".to_string(),
            ));
        }

        Ok(Self {
            start_hour,
            start_minute,
            end_hour,
            end_minute,
            days,
        })
    }

    /// Check if the given time falls within peak hours
    pub fn is_peak_time(&self, time: DateTime<Utc>) -> bool {
        let current_minutes = time.hour() * 60 + time.minute();
        let start_minutes = self.start_hour as u32 * 60 + self.start_minute as u32;
        let end_minutes = self.end_hour as u32 * 60 + self.end_minute as u32;

        // Check if today is in the allowed days
        if let Some(ref days) = self.days {
            if !days.contains(&time.weekday()) {
                return false;
            }
        }

        // Handle wrap-around (e.g., 22:00 to 06:00)
        if start_minutes <= end_minutes {
            current_minutes >= start_minutes && current_minutes < end_minutes
        } else {
            current_minutes >= start_minutes || current_minutes < end_minutes
        }
    }

    /// Format as human-readable string
    pub fn format(&self) -> String {
        let days_str = match &self.days {
            None => "every day".to_string(),
            Some(days) => {
                if days.is_empty() {
                    "no days".to_string()
                } else {
                    days.iter()
                        .map(|d| format!("{:?}", d))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            }
        };

        format!(
            "{:02}:{:02}-{:02}:{:02} ({})",
            self.start_hour, self.start_minute, self.end_hour, self.end_minute, days_str
        )
    }
}

/// Auto-pause configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPauseConfig {
    /// Whether auto-pause is enabled
    pub enabled: bool,
    /// Peak hours definition (when to pause downloads)
    pub peak_hours: Option<PeakHours>,
    /// Whether to auto-resume when peak hours end
    pub auto_resume: bool,
    /// Reason to set on paused tasks (for user visibility)
    pub pause_reason: String,
}

impl Default for AutoPauseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            peak_hours: None,
            auto_resume: true,
            pause_reason: "Auto-paused during peak hours".to_string(),
        }
    }
}

impl AutoPauseConfig {
    /// Create a new configuration with peak hours
    pub fn new(peak_hours: PeakHours) -> Self {
        Self {
            enabled: true,
            peak_hours: Some(peak_hours),
            auto_resume: true,
            pause_reason: "Auto-paused during peak hours".to_string(),
        }
    }
}

/// Status of auto-pause system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPauseStatus {
    /// Whether auto-pause is enabled
    pub enabled: bool,
    /// Current peak hours configuration
    pub peak_hours: Option<PeakHours>,
    /// Whether auto-resume is enabled
    pub auto_resume: bool,
    /// Whether we're currently in peak hours
    pub is_peak_time: bool,
    /// Number of tasks currently paused by auto-pause
    pub paused_task_count: usize,
    /// Time when peak hours started (if currently in peak)
    pub peak_started_at: Option<DateTime<Utc>>,
}

/// Save auto-pause configuration to disk
pub async fn save_auto_pause_config(
    config: &AutoPauseConfig,
    data_dir: &Path,
) -> Result<(), AutoPauseError> {
    let config_path = data_dir.join("auto_pause_config.json");
    let config_json = serde_json::to_string_pretty(config)?;
    fs::write(&config_path, config_json).await?;
    Ok(())
}

/// Load auto-pause configuration from disk
pub async fn load_auto_pause_config(
    data_dir: &Path,
) -> Result<Option<AutoPauseConfig>, AutoPauseError> {
    let config_path = data_dir.join("auto_pause_config.json");
    if !config_path.exists() {
        return Ok(None);
    }
    let config_json = fs::read_to_string(&config_path).await?;
    let config: AutoPauseConfig = serde_json::from_str(&config_json)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_peak_hours_creation() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        assert_eq!(peak.start_hour, 9);
        assert_eq!(peak.end_hour, 17);
    }

    #[test]
    fn test_peak_hours_invalid_time() {
        assert!(PeakHours::new(25, 0, 17, 0, None).is_err());
        assert!(PeakHours::new(9, 0, 17, 61, None).is_err());
    }

    #[test]
    fn test_is_peak_time_simple() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();

        // 10:00 is within 9:00-17:00
        let time = Utc.with_ymd_and_hms(2026, 8, 9, 10, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 8:59 is before peak
        let time = Utc.with_ymd_and_hms(2026, 8, 9, 8, 59, 0).unwrap();
        assert!(!peak.is_peak_time(time));

        // 17:00 is after peak (exclusive end)
        let time = Utc.with_ymd_and_hms(2026, 8, 9, 17, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));

        // 16:59 is within peak
        let time = Utc.with_ymd_and_hms(2026, 8, 9, 16, 59, 0).unwrap();
        assert!(peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_with_days() {
        let peak = PeakHours::new(
            9,
            0,
            17,
            0,
            Some(vec![Weekday::Mon, Weekday::Tue, Weekday::Wed]),
        )
        .unwrap();

        // Monday 10:00 - should be peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap(); // Monday
        assert!(peak.is_peak_time(time));

        // Thursday 10:00 - should not be peak
        let time = Utc.with_ymd_and_hms(2026, 8, 13, 10, 0, 0).unwrap(); // Thursday
        assert!(!peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_wrap_around() {
        // 22:00 to 06:00 (overnight)
        let peak = PeakHours::new(22, 0, 6, 0, None).unwrap();

        // 23:00 - within peak
        let time = Utc.with_ymd_and_hms(2026, 8, 9, 23, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 02:00 - within peak (next day)
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 2, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 12:00 - not within peak
        let time = Utc.with_ymd_and_hms(2026, 8, 9, 12, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    #[test]
    fn test_format_peak_hours() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        assert_eq!(peak.format(), "09:00-17:00 (every day)");

        let peak = PeakHours::new(22, 0, 6, 0, Some(vec![Weekday::Sat, Weekday::Sun])).unwrap();
        assert_eq!(peak.format(), "22:00-06:00 (Sat, Sun)");
    }

    #[test]
    fn test_config_default() {
        let config = AutoPauseConfig::default();
        assert!(!config.enabled);
        assert!(config.peak_hours.is_none());
        assert!(config.auto_resume);
    }

    #[test]
    fn test_config_new() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        let config = AutoPauseConfig::new(peak);
        assert!(config.enabled);
        assert!(config.peak_hours.is_some());
        assert!(config.auto_resume);
    }

    #[test]
    fn test_save_load_config() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
            let config = AutoPauseConfig::new(peak);

            // Save
            save_auto_pause_config(&config, temp_dir.path())
                .await
                .unwrap();

            // Load
            let loaded = load_auto_pause_config(temp_dir.path())
                .await
                .unwrap()
                .unwrap();

            assert!(loaded.enabled);
            assert!(loaded.peak_hours.is_some());
            let loaded_peak = loaded.peak_hours.unwrap();
            assert_eq!(loaded_peak.start_hour, 9);
            assert_eq!(loaded_peak.end_hour, 17);
        });
    }

    #[test]
    fn test_load_nonexistent_config() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let result = load_auto_pause_config(temp_dir.path()).await.unwrap();
            assert!(result.is_none());
        });
    }
}
