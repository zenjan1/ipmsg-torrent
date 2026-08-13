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
        if let Some(ref days) = self.days
            && !days.contains(&time.weekday())
        {
            return false;
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

    // ===== PeakHours Serialization =====

    #[test]
    fn test_peak_hours_serialization_roundtrip() {
        let peak = PeakHours::new(9, 30, 17, 30, Some(vec![Weekday::Mon, Weekday::Fri])).unwrap();
        let json = serde_json::to_string(&peak).unwrap();
        let back: PeakHours = serde_json::from_str(&json).unwrap();
        assert_eq!(back.start_hour, 9);
        assert_eq!(back.start_minute, 30);
        assert_eq!(back.end_hour, 17);
        assert_eq!(back.end_minute, 30);
        assert_eq!(back.days.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_peak_hours_serialization_no_days() {
        let peak = PeakHours::new(0, 0, 23, 59, None).unwrap();
        let json = serde_json::to_string(&peak).unwrap();
        let back: PeakHours = serde_json::from_str(&json).unwrap();
        assert!(back.days.is_none());
    }

    #[test]
    fn test_peak_hours_extra_fields_tolerated() {
        let json = r#"{"start_hour":9,"start_minute":0,"end_hour":17,"end_minute":0,"days":null,"extra":"ignored"}"#;
        let peak: PeakHours = serde_json::from_str(json).unwrap();
        assert_eq!(peak.start_hour, 9);
        assert_eq!(peak.end_hour, 17);
    }

    // ===== AutoPauseConfig Serialization =====

    #[test]
    fn test_config_serialization_roundtrip() {
        let peak = PeakHours::new(8, 0, 20, 0, None).unwrap();
        let config = AutoPauseConfig {
            enabled: true,
            peak_hours: Some(peak),
            auto_resume: false,
            pause_reason: "Custom reason".to_string(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: AutoPauseConfig = serde_json::from_str(&json).unwrap();
        assert!(back.enabled);
        assert!(back.peak_hours.is_some());
        assert!(!back.auto_resume);
        assert_eq!(back.pause_reason, "Custom reason");
    }

    #[test]
    fn test_config_serialization_no_peak_hours() {
        let config = AutoPauseConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: AutoPauseConfig = serde_json::from_str(&json).unwrap();
        assert!(!back.enabled);
        assert!(back.peak_hours.is_none());
        assert!(back.auto_resume);
    }

    // ===== AutoPauseStatus Serialization =====

    #[test]
    fn test_status_serialization_roundtrip() {
        let status = AutoPauseStatus {
            enabled: true,
            peak_hours: Some(PeakHours::new(9, 0, 17, 0, None).unwrap()),
            auto_resume: true,
            is_peak_time: false,
            paused_task_count: 5,
            peak_started_at: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: AutoPauseStatus = serde_json::from_str(&json).unwrap();
        assert!(back.enabled);
        assert_eq!(back.paused_task_count, 5);
        assert!(!back.is_peak_time);
    }

    #[test]
    fn test_status_with_peak_started_at() {
        let started_at = Utc.with_ymd_and_hms(2026, 8, 13, 9, 0, 0).unwrap();
        let status = AutoPauseStatus {
            enabled: true,
            peak_hours: Some(PeakHours::new(9, 0, 17, 0, None).unwrap()),
            auto_resume: true,
            is_peak_time: true,
            paused_task_count: 3,
            peak_started_at: Some(started_at),
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: AutoPauseStatus = serde_json::from_str(&json).unwrap();
        assert!(back.peak_started_at.is_some());
        assert!(back.is_peak_time);
    }

    // ===== PeakHours Validation =====

    #[test]
    fn test_peak_hours_valid_boundaries() {
        // All valid boundary values
        assert!(PeakHours::new(0, 0, 0, 0, None).is_ok());
        assert!(PeakHours::new(0, 0, 23, 59, None).is_ok());
        assert!(PeakHours::new(23, 59, 23, 59, None).is_ok());
        assert!(PeakHours::new(12, 30, 13, 30, None).is_ok());
    }

    #[test]
    fn test_peak_hours_invalid_start_hour() {
        let err = PeakHours::new(24, 0, 17, 0, None).unwrap_err();
        assert!(err.to_string().contains("Hour must be 0-23"));
    }

    #[test]
    fn test_peak_hours_invalid_end_hour() {
        let err = PeakHours::new(9, 0, 25, 0, None).unwrap_err();
        assert!(err.to_string().contains("Hour must be 0-23"));
    }

    #[test]
    fn test_peak_hours_invalid_start_minute() {
        let err = PeakHours::new(9, 60, 17, 0, None).unwrap_err();
        assert!(err.to_string().contains("Minute must be 0-59"));
    }

    #[test]
    fn test_peak_hours_invalid_end_minute() {
        let err = PeakHours::new(9, 0, 17, 60, None).unwrap_err();
        assert!(err.to_string().contains("Minute must be 0-59"));
    }

    #[test]
    fn test_peak_hours_multiple_invalid_returns_first() {
        // Both hours invalid, should return error for start_hour
        let result = PeakHours::new(25, 0, 30, 0, None);
        assert!(result.is_err());
    }

    // ===== is_peak_time Boundary Tests =====

    #[test]
    fn test_is_peak_time_exact_start() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 9, 0, 0).unwrap();
        assert!(peak.is_peak_time(time)); // start is inclusive
    }

    #[test]
    fn test_is_peak_time_exact_end() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 17, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time)); // end is exclusive
    }

    #[test]
    fn test_is_peak_time_one_minute_before_end() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 16, 59, 0).unwrap();
        assert!(peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_one_minute_before_start() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 8, 59, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_midnight_boundary() {
        // Peak hours spanning midnight: 23:00 to 01:00
        let peak = PeakHours::new(23, 0, 1, 0, None).unwrap();

        // 23:00 - start of peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 23, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 23:59 - within peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 23, 59, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 00:00 - within peak (after midnight)
        let time = Utc.with_ymd_and_hms(2026, 8, 11, 0, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 00:59 - within peak
        let time = Utc.with_ymd_and_hms(2026, 8, 11, 0, 59, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 01:00 - end of peak (exclusive)
        let time = Utc.with_ymd_and_hms(2026, 8, 11, 1, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));

        // 12:00 - not in peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_full_day() {
        // 00:00 to 23:59 covers almost entire day
        let peak = PeakHours::new(0, 0, 23, 59, None).unwrap();
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_with_minutes() {
        let peak = PeakHours::new(9, 30, 17, 15, None).unwrap();

        // 9:29 - before peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 9, 29, 0).unwrap();
        assert!(!peak.is_peak_time(time));

        // 9:30 - start of peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 9, 30, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 17:14 - within peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 17, 14, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // 17:15 - end of peak (exclusive)
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 17, 15, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    // ===== is_peak_time with Days =====

    #[test]
    fn test_is_peak_time_single_day_match() {
        let peak = PeakHours::new(9, 0, 17, 0, Some(vec![Weekday::Wed])).unwrap();
        // 2026-08-12 is a Wednesday
        let time = Utc.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_single_day_no_match() {
        let peak = PeakHours::new(9, 0, 17, 0, Some(vec![Weekday::Wed])).unwrap();
        // 2026-08-10 is a Monday
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_empty_days_never_matches() {
        let peak = PeakHours::new(9, 0, 17, 0, Some(vec![])).unwrap();
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time)); // No days = never peak
    }

    #[test]
    fn test_is_peak_time_all_weekdays() {
        let days = vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ];
        let peak = PeakHours::new(9, 0, 17, 0, Some(days)).unwrap();

        // Monday 10:00 - peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // Friday 10:00 - peak
        let time = Utc.with_ymd_and_hms(2026, 8, 14, 10, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // Saturday 10:00 - not peak
        let time = Utc.with_ymd_and_hms(2026, 8, 15, 10, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));

        // Sunday 10:00 - not peak
        let time = Utc.with_ymd_and_hms(2026, 8, 16, 10, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    #[test]
    fn test_is_peak_time_weekend_only() {
        let peak = PeakHours::new(10, 0, 22, 0, Some(vec![Weekday::Sat, Weekday::Sun])).unwrap();

        // Saturday 15:00 - peak
        let time = Utc.with_ymd_and_hms(2026, 8, 15, 15, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // Sunday 15:00 - peak
        let time = Utc.with_ymd_and_hms(2026, 8, 16, 15, 0, 0).unwrap();
        assert!(peak.is_peak_time(time));

        // Monday 15:00 - not peak
        let time = Utc.with_ymd_and_hms(2026, 8, 10, 15, 0, 0).unwrap();
        assert!(!peak.is_peak_time(time));
    }

    // ===== format() =====

    #[test]
    fn test_format_no_days() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        assert_eq!(peak.format(), "09:00-17:00 (every day)");
    }

    #[test]
    fn test_format_empty_days() {
        let peak = PeakHours::new(9, 0, 17, 0, Some(vec![])).unwrap();
        assert_eq!(peak.format(), "09:00-17:00 (no days)");
    }

    #[test]
    fn test_format_single_day() {
        let peak = PeakHours::new(9, 0, 17, 0, Some(vec![Weekday::Mon])).unwrap();
        assert_eq!(peak.format(), "09:00-17:00 (Mon)");
    }

    #[test]
    fn test_format_multiple_days() {
        let peak = PeakHours::new(
            22,
            30,
            6,
            15,
            Some(vec![Weekday::Mon, Weekday::Wed, Weekday::Fri]),
        )
        .unwrap();
        let formatted = peak.format();
        assert!(formatted.contains("22:30-06:15"));
        assert!(formatted.contains("Mon"));
        assert!(formatted.contains("Wed"));
        assert!(formatted.contains("Fri"));
    }

    #[test]
    fn test_format_zero_padded() {
        let peak = PeakHours::new(1, 5, 3, 7, None).unwrap();
        assert_eq!(peak.format(), "01:05-03:07 (every day)");
    }

    // ===== AutoPauseConfig =====

    #[test]
    fn test_config_default_values() {
        let config = AutoPauseConfig::default();
        assert!(!config.enabled);
        assert!(config.peak_hours.is_none());
        assert!(config.auto_resume);
        assert_eq!(config.pause_reason, "Auto-paused during peak hours");
    }

    #[test]
    fn test_config_new_with_peak_hours() {
        let peak = PeakHours::new(8, 0, 18, 0, None).unwrap();
        let config = AutoPauseConfig::new(peak);
        assert!(config.enabled);
        assert!(config.peak_hours.is_some());
        assert!(config.auto_resume);
        let p = config.peak_hours.unwrap();
        assert_eq!(p.start_hour, 8);
        assert_eq!(p.end_hour, 18);
    }

    #[test]
    fn test_config_custom_pause_reason() {
        let config = AutoPauseConfig {
            enabled: true,
            peak_hours: None,
            auto_resume: false,
            pause_reason: "Gaming time".to_string(),
        };
        assert_eq!(config.pause_reason, "Gaming time");
        assert!(!config.auto_resume);
    }

    // ===== Error Display =====

    #[test]
    fn test_error_json_display() {
        let err = AutoPauseError::Json(serde_json::from_str::<AutoPauseConfig>("bad").unwrap_err());
        let msg = format!("{err}");
        assert!(msg.contains("JSON error"));
    }

    #[test]
    fn test_error_io_display() {
        let err = AutoPauseError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "denied",
        ));
        let msg = format!("{err}");
        assert!(msg.contains("I/O error"));
        assert!(msg.contains("denied"));
    }

    #[test]
    fn test_error_invalid_time_display() {
        let err = AutoPauseError::InvalidTime("bad hour".to_string());
        let msg = format!("{err}");
        assert!(msg.contains("Invalid time format"));
        assert!(msg.contains("bad hour"));
    }

    #[test]
    fn test_error_debug_variants() {
        let err1 = AutoPauseError::InvalidTime("test".to_string());
        assert!(format!("{err1:?}").contains("InvalidTime"));

        let err2 = AutoPauseError::Io(std::io::Error::new(std::io::ErrorKind::Other, "x"));
        assert!(format!("{err2:?}").contains("Io"));
    }

    // ===== Traits =====

    #[test]
    fn test_peak_hours_clone() {
        let peak = PeakHours::new(9, 0, 17, 0, Some(vec![Weekday::Mon])).unwrap();
        let cloned = peak.clone();
        assert_eq!(cloned.start_hour, peak.start_hour);
        assert_eq!(cloned.end_hour, peak.end_hour);
        assert_eq!(cloned.days, peak.days);
    }

    #[test]
    fn test_peak_hours_debug() {
        let peak = PeakHours::new(9, 0, 17, 0, None).unwrap();
        let debug = format!("{peak:?}");
        assert!(debug.contains("PeakHours"));
        assert!(debug.contains("start_hour"));
    }

    #[test]
    fn test_config_clone() {
        let config = AutoPauseConfig {
            enabled: true,
            peak_hours: Some(PeakHours::new(9, 0, 17, 0, None).unwrap()),
            auto_resume: false,
            pause_reason: "test".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.pause_reason, config.pause_reason);
    }

    #[test]
    fn test_config_debug() {
        let config = AutoPauseConfig::default();
        let debug = format!("{config:?}");
        assert!(debug.contains("AutoPauseConfig"));
    }

    #[test]
    fn test_status_clone() {
        let status = AutoPauseStatus {
            enabled: true,
            peak_hours: None,
            auto_resume: true,
            is_peak_time: false,
            paused_task_count: 0,
            peak_started_at: None,
        };
        let cloned = status.clone();
        assert_eq!(cloned.enabled, status.enabled);
        assert_eq!(cloned.paused_task_count, status.paused_task_count);
    }

    // ===== Persistence Edge Cases =====

    #[test]
    fn test_save_overwrites_existing_config() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();

            // Save first config
            let peak1 = PeakHours::new(9, 0, 17, 0, None).unwrap();
            let config1 = AutoPauseConfig::new(peak1);
            save_auto_pause_config(&config1, temp_dir.path())
                .await
                .unwrap();

            // Save second config (overwrite)
            let peak2 = PeakHours::new(22, 0, 6, 0, None).unwrap();
            let config2 = AutoPauseConfig {
                enabled: true,
                peak_hours: Some(peak2),
                auto_resume: false,
                pause_reason: "Night mode".to_string(),
            };
            save_auto_pause_config(&config2, temp_dir.path())
                .await
                .unwrap();

            // Load should get second config
            let loaded = load_auto_pause_config(temp_dir.path())
                .await
                .unwrap()
                .unwrap();
            let loaded_peak = loaded.peak_hours.unwrap();
            assert_eq!(loaded_peak.start_hour, 22);
            assert_eq!(loaded_peak.end_hour, 6);
            assert!(!loaded.auto_resume);
        });
    }

    #[test]
    fn test_load_corrupt_json() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let config_path = temp_dir.path().join("auto_pause_config.json");
            tokio::fs::write(&config_path, b"not valid json{{")
                .await
                .unwrap();
            let result = load_auto_pause_config(temp_dir.path()).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_save_creates_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = AutoPauseConfig::default();
            save_auto_pause_config(&config, temp_dir.path())
                .await
                .unwrap();
            let config_path = temp_dir.path().join("auto_pause_config.json");
            assert!(config_path.exists());
        });
    }

    #[test]
    fn test_persistence_with_days_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let peak = PeakHours::new(
                9,
                0,
                17,
                0,
                Some(vec![
                    Weekday::Mon,
                    Weekday::Tue,
                    Weekday::Wed,
                    Weekday::Thu,
                    Weekday::Fri,
                ]),
            )
            .unwrap();
            let config = AutoPauseConfig::new(peak);
            save_auto_pause_config(&config, temp_dir.path())
                .await
                .unwrap();
            let loaded = load_auto_pause_config(temp_dir.path())
                .await
                .unwrap()
                .unwrap();
            let loaded_peak = loaded.peak_hours.unwrap();
            assert_eq!(loaded_peak.days.as_ref().unwrap().len(), 5);
        });
    }

    // ===== Complete Workflow =====

    #[test]
    fn test_complete_workflow() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();

            // 1. Start with no config
            assert!(
                load_auto_pause_config(temp_dir.path())
                    .await
                    .unwrap()
                    .is_none()
            );

            // 2. Create and save config
            let peak = PeakHours::new(9, 0, 17, 0, Some(vec![Weekday::Mon, Weekday::Fri])).unwrap();
            let config = AutoPauseConfig {
                enabled: true,
                peak_hours: Some(peak),
                auto_resume: true,
                pause_reason: "Work hours".to_string(),
            };
            save_auto_pause_config(&config, temp_dir.path())
                .await
                .unwrap();

            // 3. Load and verify
            let loaded = load_auto_pause_config(temp_dir.path())
                .await
                .unwrap()
                .unwrap();
            assert!(loaded.enabled);
            assert!(loaded.auto_resume);
            assert_eq!(loaded.pause_reason, "Work hours");
            let p = loaded.peak_hours.unwrap();
            assert_eq!(p.start_hour, 9);
            assert_eq!(p.days.as_ref().unwrap().len(), 2);

            // 4. Verify peak time check works
            // Monday 10:00 - should be peak
            let time = Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
            assert!(p.is_peak_time(time));

            // Tuesday 10:00 - should NOT be peak (only Mon/Fri)
            let time = Utc.with_ymd_and_hms(2026, 8, 11, 10, 0, 0).unwrap();
            assert!(!p.is_peak_time(time));
        });
    }
}
