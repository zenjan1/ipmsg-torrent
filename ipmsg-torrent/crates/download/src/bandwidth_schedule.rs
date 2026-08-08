//! Bandwidth scheduling for automatic speed limit adjustment
//!
//! Allows users to define time-based rules that automatically adjust
//! the global download speed limit. For example:
//! - Limit to 1MB/s during work hours (9am-6pm)
//! - Unlimited during night hours (10pm-8am)
//! - 5MB/s during evening hours
//!
//! Rules are evaluated in priority order, and the first matching rule
//! is applied. Rules persist to disk and are restored on startup.

use chrono::{Datelike, Local, Timelike, Weekday};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

/// A bandwidth schedule rule that applies during specific time windows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthScheduleRule {
    /// Unique rule identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Start hour (0-23)
    pub start_hour: u32,
    /// Start minute (0-59)
    pub start_minute: u32,
    /// End hour (0-23)
    pub end_hour: u32,
    /// End minute (0-59)
    pub end_minute: u32,
    /// Speed limit in bytes per second (0 = unlimited)
    pub speed_limit_bps: u64,
    /// Days of week when this rule applies (empty = every day)
    pub days_of_week: Vec<Weekday>,
    /// Priority (higher number = higher priority)
    pub priority: i32,
    /// Whether this rule is enabled
    pub enabled: bool,
}

impl BandwidthScheduleRule {
    /// Create a new rule with the given parameters
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        start_hour: u32,
        start_minute: u32,
        end_hour: u32,
        end_minute: u32,
        speed_limit_bps: u64,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            start_hour,
            start_minute,
            end_hour,
            end_minute,
            speed_limit_bps,
            days_of_week: Vec::new(),
            priority: 0,
            enabled: true,
        }
    }

    /// Set the days of week when this rule applies
    pub fn with_days(mut self, days: Vec<Weekday>) -> Self {
        self.days_of_week = days;
        self
    }

    /// Set the priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Check if this rule matches the current time
    pub fn matches_now(&self) -> bool {
        self.matches_time(&Local::now())
    }

    /// Check if this rule matches the given time
    pub fn matches_time(&self, time: &chrono::DateTime<Local>) -> bool {
        if !self.enabled {
            return false;
        }

        // Check day of week
        if !self.days_of_week.is_empty() && !self.days_of_week.contains(&time.weekday()) {
            return false;
        }

        // Check time window
        let current_minutes = time.hour() * 60 + time.minute();
        let start_minutes = self.start_hour * 60 + self.start_minute;
        let end_minutes = self.end_hour * 60 + self.end_minute;

        if start_minutes <= end_minutes {
            // Normal window (e.g., 9:00-17:00)
            current_minutes >= start_minutes && current_minutes < end_minutes
        } else {
            // Overnight window (e.g., 22:00-06:00)
            current_minutes >= start_minutes || current_minutes < end_minutes
        }
    }

    /// Format the time window for display
    pub fn format_time_window(&self) -> String {
        format!(
            "{:02}:{:02}-{:02}:{:02}",
            self.start_hour, self.start_minute, self.end_hour, self.end_minute
        )
    }

    /// Format the days of week for display
    pub fn format_days(&self) -> String {
        if self.days_of_week.is_empty() {
            "every day".to_string()
        } else {
            self.days_of_week
                .iter()
                .map(|d| format!("{:?}", d))
                .collect::<Vec<_>>()
                .join(", ")
        }
    }

    /// Format the speed limit for display
    pub fn format_speed_limit(&self) -> String {
        if self.speed_limit_bps == 0 {
            "unlimited".to_string()
        } else {
            format_speed_bps(self.speed_limit_bps)
        }
    }
}

/// Format bytes per second to human-readable string
fn format_speed_bps(bps: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bps == 0 {
        return "unlimited".to_string();
    }
    if bps >= GB {
        format!("{:.1} GB/s", bps as f64 / GB as f64)
    } else if bps >= MB {
        format!("{:.1} MB/s", bps as f64 / MB as f64)
    } else if bps >= KB {
        format!("{:.1} KB/s", bps as f64 / KB as f64)
    } else {
        format!("{} B/s", bps)
    }
}

/// Parse a speed limit string to bytes per second
///
/// Supports formats like:
/// - "100KB/s" or "100kb"
/// - "5MB/s" or "5mb"
/// - "1GB/s" or "1gb"
/// - "0" or "unlimited" for no limit
pub fn parse_speed_limit(s: &str) -> Option<u64> {
    let s = s.trim().to_lowercase();

    if s == "0" || s == "unlimited" || s == "none" {
        return Some(0);
    }

    // Try to parse as plain number (bytes per second)
    if let Ok(bps) = s.parse::<u64>() {
        return Some(bps);
    }

    // Parse with unit suffix
    let (num_str, multiplier) = if s.ends_with("kb/s") || s.ends_with("kb") {
        (
            &s[..s.len() - if s.ends_with("/s") { 4 } else { 2 }],
            1024u64,
        )
    } else if s.ends_with("mb/s") || s.ends_with("mb") {
        (
            &s[..s.len() - if s.ends_with("/s") { 4 } else { 2 }],
            1024 * 1024,
        )
    } else if s.ends_with("gb/s") || s.ends_with("gb") {
        (
            &s[..s.len() - if s.ends_with("/s") { 4 } else { 2 }],
            1024 * 1024 * 1024,
        )
    } else if s.ends_with("b/s") || s.ends_with("b") {
        (&s[..s.len() - if s.ends_with("/s") { 3 } else { 1 }], 1)
    } else {
        return None;
    };

    let num: f64 = num_str.trim().parse().ok()?;
    Some((num * multiplier as f64) as u64)
}

/// Parse a time string in HH:MM format
pub fn parse_time(s: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = s.trim().split(':').collect();
    if parts.len() != 2 {
        return None;
    }

    let hour: u32 = parts[0].parse().ok()?;
    let minute: u32 = parts[1].parse().ok()?;

    if hour > 23 || minute > 59 {
        return None;
    }

    Some((hour, minute))
}

/// Parse a time window string like "09:00-17:00"
pub fn parse_time_window(s: &str) -> Option<(u32, u32, u32, u32)> {
    let parts: Vec<&str> = s.trim().split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let (start_h, start_m) = parse_time(parts[0])?;
    let (end_h, end_m) = parse_time(parts[1])?;

    Some((start_h, start_m, end_h, end_m))
}

/// Parse days of week from comma-separated string
///
/// Supports: "mon,tue,wed" or "monday,tuesday" or "1,2,3" (1=Monday)
pub fn parse_days(s: &str) -> Option<Vec<Weekday>> {
    if s.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut days = Vec::new();
    for part in s.split(',') {
        let part = part.trim().to_lowercase();
        let day = match part.as_str() {
            "mon" | "monday" | "1" => Weekday::Mon,
            "tue" | "tuesday" | "2" => Weekday::Tue,
            "wed" | "wednesday" | "3" => Weekday::Wed,
            "thu" | "thursday" | "4" => Weekday::Thu,
            "fri" | "friday" | "5" => Weekday::Fri,
            "sat" | "saturday" | "6" => Weekday::Sat,
            "sun" | "sunday" | "7" => Weekday::Sun,
            _ => return None,
        };
        days.push(day);
    }

    Some(days)
}

/// Bandwidth schedule manager
#[derive(Debug, Default)]
pub struct BandwidthScheduleManager {
    rules: Vec<BandwidthScheduleRule>,
}

impl BandwidthScheduleManager {
    /// Create a new empty manager
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a rule
    pub fn add_rule(&mut self, rule: BandwidthScheduleRule) {
        self.rules.push(rule);
        self.sort_rules();
    }

    /// Remove a rule by ID
    pub fn remove_rule(&mut self, id: &str) -> bool {
        let initial_len = self.rules.len();
        self.rules.retain(|r| r.id != id);
        self.rules.len() < initial_len
    }

    /// Get a rule by ID
    pub fn get_rule(&self, id: &str) -> Option<&BandwidthScheduleRule> {
        self.rules.iter().find(|r| r.id == id)
    }

    /// List all rules
    pub fn list_rules(&self) -> &[BandwidthScheduleRule] {
        &self.rules
    }

    /// Sort rules by priority (descending)
    fn sort_rules(&mut self) {
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Find the first matching rule for the current time
    pub fn find_matching_rule(&self) -> Option<&BandwidthScheduleRule> {
        self.rules.iter().find(|r| r.matches_now())
    }

    /// Find the first matching rule for the given time
    pub fn find_matching_rule_at(
        &self,
        time: &chrono::DateTime<Local>,
    ) -> Option<&BandwidthScheduleRule> {
        self.rules.iter().find(|r| r.matches_time(time))
    }

    /// Get the speed limit that should be applied right now
    ///
    /// Returns None if no rule matches (use default/unlimited)
    pub fn current_speed_limit(&self) -> Option<u64> {
        self.find_matching_rule().map(|r| r.speed_limit_bps)
    }

    /// Get the speed limit that should be applied at the given time
    pub fn speed_limit_at(&self, time: &chrono::DateTime<Local>) -> Option<u64> {
        self.find_matching_rule_at(time).map(|r| r.speed_limit_bps)
    }

    /// Clear all rules
    pub fn clear(&mut self) {
        self.rules.clear();
    }

    /// Get the number of rules
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Check if there are no rules
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

/// Save bandwidth schedule to disk
pub async fn save_bandwidth_schedule(
    manager: &BandwidthScheduleManager,
    data_dir: &Path,
) -> Result<(), BandwidthScheduleError> {
    let path = data_dir.join("bandwidth_schedule.json");
    let json = serde_json::to_string_pretty(manager.list_rules())
        .map_err(|e| BandwidthScheduleError::Serialize(e.to_string()))?;

    fs::write(&path, json)
        .await
        .map_err(|e| BandwidthScheduleError::Io(e.to_string()))?;

    Ok(())
}

/// Load bandwidth schedule from disk
pub async fn load_bandwidth_schedule(
    data_dir: &Path,
) -> Result<Option<BandwidthScheduleManager>, BandwidthScheduleError> {
    let path = data_dir.join("bandwidth_schedule.json");

    if !path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(&path)
        .await
        .map_err(|e| BandwidthScheduleError::Io(e.to_string()))?;

    let rules: Vec<BandwidthScheduleRule> = serde_json::from_str(&json)
        .map_err(|e| BandwidthScheduleError::Deserialize(e.to_string()))?;

    let mut manager = BandwidthScheduleManager::new();
    for rule in rules {
        manager.add_rule(rule);
    }

    Ok(Some(manager))
}

/// Errors that can occur during bandwidth schedule operations
#[derive(Debug, thiserror::Error)]
pub enum BandwidthScheduleError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("Serialization error: {0}")]
    Serialize(String),
    #[error("Deserialization error: {0}")]
    Deserialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_parse_speed_limit() {
        assert_eq!(parse_speed_limit("0"), Some(0));
        assert_eq!(parse_speed_limit("unlimited"), Some(0));
        assert_eq!(parse_speed_limit("100"), Some(100));
        assert_eq!(parse_speed_limit("100KB/s"), Some(100 * 1024));
        assert_eq!(parse_speed_limit("5MB/s"), Some(5 * 1024 * 1024));
        assert_eq!(parse_speed_limit("1GB/s"), Some(1024 * 1024 * 1024));
        assert_eq!(
            parse_speed_limit("1.5MB/s"),
            Some((1.5 * 1024.0 * 1024.0) as u64)
        );
        assert_eq!(parse_speed_limit("invalid"), None);
    }

    #[test]
    fn test_parse_time() {
        assert_eq!(parse_time("09:00"), Some((9, 0)));
        assert_eq!(parse_time("17:30"), Some((17, 30)));
        assert_eq!(parse_time("23:59"), Some((23, 59)));
        assert_eq!(parse_time("25:00"), None);
        assert_eq!(parse_time("12:60"), None);
        assert_eq!(parse_time("invalid"), None);
    }

    #[test]
    fn test_parse_time_window() {
        assert_eq!(parse_time_window("09:00-17:00"), Some((9, 0, 17, 0)));
        assert_eq!(parse_time_window("22:00-06:00"), Some((22, 0, 6, 0)));
        assert_eq!(parse_time_window("invalid"), None);
    }

    #[test]
    fn test_parse_days() {
        assert_eq!(parse_days(""), Some(Vec::new()));
        assert_eq!(
            parse_days("mon,tue,wed"),
            Some(vec![Weekday::Mon, Weekday::Tue, Weekday::Wed])
        );
        assert_eq!(
            parse_days("monday,friday"),
            Some(vec![Weekday::Mon, Weekday::Fri])
        );
        assert_eq!(
            parse_days("1,3,5"),
            Some(vec![Weekday::Mon, Weekday::Wed, Weekday::Fri])
        );
        assert_eq!(parse_days("invalid"), None);
    }

    #[test]
    fn test_rule_matches_time_normal_window() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 0, 17, 0, 1024 * 1024);

        // Before window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 8, 59, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // Start of window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 9, 0, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Middle of window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert!(rule.matches_time(&time));

        // End of window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 17, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // After window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 17, 1, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_time_overnight_window() {
        let rule = BandwidthScheduleRule::new("1", "test", 22, 0, 6, 0, 0);

        // Before window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 21, 59, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // Start of window
        let time = Local.with_ymd_and_hms(2026, 1, 1, 22, 0, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Middle of window (late night)
        let time = Local.with_ymd_and_hms(2026, 1, 1, 23, 30, 0).unwrap();
        assert!(rule.matches_time(&time));

        // After midnight
        let time = Local.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Early morning
        let time = Local.with_ymd_and_hms(2026, 1, 2, 5, 59, 0).unwrap();
        assert!(rule.matches_time(&time));

        // End of window
        let time = Local.with_ymd_and_hms(2026, 1, 2, 6, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_days() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024).with_days(vec![
            Weekday::Mon,
            Weekday::Wed,
            Weekday::Fri,
        ]);

        // Monday (2026-01-05 is a Monday)
        let time = Local.with_ymd_and_hms(2026, 1, 5, 12, 0, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Tuesday
        let time = Local.with_ymd_and_hms(2026, 1, 6, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // Wednesday
        let time = Local.with_ymd_and_hms(2026, 1, 7, 12, 0, 0).unwrap();
        assert!(rule.matches_time(&time));
    }

    #[test]
    fn test_rule_disabled() {
        let mut rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        rule.enabled = false;

        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(0), "unlimited");
        assert_eq!(format_speed_bps(512), "512 B/s");
        assert_eq!(format_speed_bps(1024), "1.0 KB/s");
        assert_eq!(format_speed_bps(1536), "1.5 KB/s");
        assert_eq!(format_speed_bps(1024 * 1024), "1.0 MB/s");
        assert_eq!(format_speed_bps(5 * 1024 * 1024), "5.0 MB/s");
        assert_eq!(format_speed_bps(1024 * 1024 * 1024), "1.0 GB/s");
    }

    #[test]
    fn test_manager_priority() {
        let mut manager = BandwidthScheduleManager::new();

        let rule1 = BandwidthScheduleRule::new("1", "low", 0, 0, 23, 59, 1024).with_priority(1);
        let rule2 =
            BandwidthScheduleRule::new("2", "high", 0, 0, 23, 59, 1024 * 1024).with_priority(10);

        manager.add_rule(rule1);
        manager.add_rule(rule2);

        // Higher priority rule should match first
        let matching = manager.find_matching_rule();
        assert!(matching.is_some());
        assert_eq!(matching.unwrap().id, "2");
    }

    #[test]
    fn test_manager_remove() {
        let mut manager = BandwidthScheduleManager::new();

        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        manager.add_rule(rule);

        assert_eq!(manager.len(), 1);
        assert!(manager.remove_rule("1"));
        assert_eq!(manager.len(), 0);
        assert!(!manager.remove_rule("nonexistent"));
    }

    #[test]
    fn test_rule_format_time_window() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 0, 17, 30, 1024);
        assert_eq!(rule.format_time_window(), "09:00-17:30");
    }

    #[test]
    fn test_rule_format_days() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        assert_eq!(rule.format_days(), "every day");

        let rule = rule.with_days(vec![Weekday::Mon, Weekday::Fri]);
        assert_eq!(rule.format_days(), "Mon, Fri");
    }

    #[test]
    fn test_rule_format_speed_limit() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 0);
        assert_eq!(rule.format_speed_limit(), "unlimited");

        let rule = BandwidthScheduleRule::new("2", "test", 0, 0, 23, 59, 5 * 1024 * 1024);
        assert_eq!(rule.format_speed_limit(), "5.0 MB/s");
    }
}
