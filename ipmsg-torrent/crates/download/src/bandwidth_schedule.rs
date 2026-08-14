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

    /// List all rules with mutable access
    pub fn list_rules_mut(&mut self) -> &mut Vec<BandwidthScheduleRule> {
        &mut self.rules
    }

    /// Sort rules by priority (descending)
    fn sort_rules(&mut self) {
        self.rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
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

    // ===== parse_speed_limit =====

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
    fn test_parse_speed_limit_none_variant() {
        assert_eq!(parse_speed_limit("none"), Some(0));
        assert_eq!(parse_speed_limit("None"), Some(0));
        assert_eq!(parse_speed_limit("NONE"), Some(0));
    }

    #[test]
    fn test_parse_speed_limit_case_insensitive() {
        assert_eq!(parse_speed_limit("Unlimited"), Some(0));
        assert_eq!(parse_speed_limit("UNLIMITED"), Some(0));
        assert_eq!(parse_speed_limit("100kb/s"), Some(100 * 1024));
        assert_eq!(parse_speed_limit("100Kb/S"), Some(100 * 1024));
        assert_eq!(parse_speed_limit("5mb"), Some(5 * 1024 * 1024));
        assert_eq!(parse_speed_limit("5MB"), Some(5 * 1024 * 1024));
        assert_eq!(parse_speed_limit("1gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_speed_limit("1GB"), Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_parse_speed_limit_b_suffix() {
        assert_eq!(parse_speed_limit("500b"), Some(500));
        assert_eq!(parse_speed_limit("500b/s"), Some(500));
        assert_eq!(parse_speed_limit("500B"), Some(500));
        assert_eq!(parse_speed_limit("500B/s"), Some(500));
    }

    #[test]
    fn test_parse_speed_limit_whitespace() {
        assert_eq!(parse_speed_limit("  100  "), Some(100));
        assert_eq!(parse_speed_limit(" 100KB/s "), Some(100 * 1024));
        assert_eq!(parse_speed_limit(" unlimited "), Some(0));
    }

    #[test]
    fn test_parse_speed_limit_invalid() {
        assert_eq!(parse_speed_limit(""), None);
        assert_eq!(parse_speed_limit("abc"), None);
        assert_eq!(parse_speed_limit("100XB/s"), None);
        assert_eq!(parse_speed_limit("KB/s"), None);
        assert_eq!(parse_speed_limit("-100"), None);
    }

    #[test]
    fn test_parse_speed_limit_fractional() {
        assert_eq!(
            parse_speed_limit("0.5MB/s"),
            Some((0.5 * 1024.0 * 1024.0) as u64)
        );
        assert_eq!(
            parse_speed_limit("2.5GB/s"),
            Some((2.5 * 1024.0 * 1024.0 * 1024.0) as u64)
        );
    }

    // ===== parse_time =====

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
    fn test_parse_time_boundaries() {
        assert_eq!(parse_time("00:00"), Some((0, 0)));
        assert_eq!(parse_time("0:0"), Some((0, 0)));
        assert_eq!(parse_time("23:59"), Some((23, 59)));
        assert_eq!(parse_time("24:00"), None);
        assert_eq!(parse_time("-1:00"), None);
        assert_eq!(parse_time("12:-1"), None);
    }

    #[test]
    fn test_parse_time_invalid_format() {
        assert_eq!(parse_time(""), None);
        assert_eq!(parse_time("12"), None);
        assert_eq!(parse_time("12:00:00"), None);
        assert_eq!(parse_time("abc:def"), None);
        assert_eq!(parse_time("12:00:extra"), None);
    }

    // ===== parse_time_window =====

    #[test]
    fn test_parse_time_window() {
        assert_eq!(parse_time_window("09:00-17:00"), Some((9, 0, 17, 0)));
        assert_eq!(parse_time_window("22:00-06:00"), Some((22, 0, 6, 0)));
        assert_eq!(parse_time_window("invalid"), None);
    }

    #[test]
    fn test_parse_time_window_boundaries() {
        assert_eq!(parse_time_window("00:00-23:59"), Some((0, 0, 23, 59)));
        assert_eq!(parse_time_window("12:30-13:30"), Some((12, 30, 13, 30)));
    }

    #[test]
    fn test_parse_time_window_invalid() {
        assert_eq!(parse_time_window(""), None);
        assert_eq!(parse_time_window("09:00"), None);
        assert_eq!(parse_time_window("09:00-"), None);
        assert_eq!(parse_time_window("-17:00"), None);
        assert_eq!(parse_time_window("25:00-17:00"), None);
        assert_eq!(parse_time_window("09:00-17:00-extra"), None);
    }

    // ===== parse_days =====

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
    fn test_parse_days_all_long_names() {
        assert_eq!(
            parse_days("monday,tuesday,wednesday,thursday,friday,saturday,sunday"),
            Some(vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
                Weekday::Sat,
                Weekday::Sun,
            ])
        );
    }

    #[test]
    fn test_parse_days_all_short_names() {
        assert_eq!(
            parse_days("mon,tue,wed,thu,fri,sat,sun"),
            Some(vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
                Weekday::Sat,
                Weekday::Sun,
            ])
        );
    }

    #[test]
    fn test_parse_days_all_numeric() {
        assert_eq!(
            parse_days("1,2,3,4,5,6,7"),
            Some(vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
                Weekday::Sat,
                Weekday::Sun,
            ])
        );
    }

    #[test]
    fn test_parse_days_case_insensitive() {
        assert_eq!(
            parse_days("MON,TUE,WED"),
            Some(vec![Weekday::Mon, Weekday::Tue, Weekday::Wed])
        );
        assert_eq!(
            parse_days("Monday,FRIDAY"),
            Some(vec![Weekday::Mon, Weekday::Fri])
        );
    }

    #[test]
    fn test_parse_days_whitespace() {
        assert_eq!(
            parse_days(" mon , tue , wed "),
            Some(vec![Weekday::Mon, Weekday::Tue, Weekday::Wed])
        );
    }

    #[test]
    fn test_parse_days_invalid() {
        assert_eq!(parse_days("xyz"), None);
        assert_eq!(parse_days("mon,xyz"), None);
        assert_eq!(parse_days("0"), None);
        assert_eq!(parse_days("8"), None);
    }

    #[test]
    fn test_parse_days_single_day() {
        assert_eq!(parse_days("mon"), Some(vec![Weekday::Mon]));
        assert_eq!(parse_days("sunday"), Some(vec![Weekday::Sun]));
        assert_eq!(parse_days("7"), Some(vec![Weekday::Sun]));
    }

    // ===== format_speed_bps =====

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
    fn test_format_speed_bps_boundary_values() {
        assert_eq!(format_speed_bps(1), "1 B/s");
        assert_eq!(format_speed_bps(1023), "1023 B/s");
        assert_eq!(format_speed_bps(1024 * 1024 - 1), "1024.0 KB/s");
        assert_eq!(format_speed_bps(1024 * 1024 * 1024 - 1), "1024.0 MB/s");
    }

    #[test]
    fn test_format_speed_bps_large_values() {
        assert_eq!(format_speed_bps(2 * 1024 * 1024 * 1024), "2.0 GB/s");
        assert_eq!(format_speed_bps(10 * 1024 * 1024 * 1024), "10.0 GB/s");
    }

    // ===== BandwidthScheduleRule construction & fields =====

    #[test]
    fn test_rule_new_defaults() {
        let rule = BandwidthScheduleRule::new("r1", "Test Rule", 9, 0, 17, 0, 1024);
        assert_eq!(rule.id, "r1");
        assert_eq!(rule.name, "Test Rule");
        assert_eq!(rule.start_hour, 9);
        assert_eq!(rule.start_minute, 0);
        assert_eq!(rule.end_hour, 17);
        assert_eq!(rule.end_minute, 0);
        assert_eq!(rule.speed_limit_bps, 1024);
        assert!(rule.days_of_week.is_empty());
        assert_eq!(rule.priority, 0);
        assert!(rule.enabled);
    }

    #[test]
    fn test_rule_with_days() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024)
            .with_days(vec![Weekday::Mon, Weekday::Fri]);
        assert_eq!(rule.days_of_week.len(), 2);
        assert!(rule.days_of_week.contains(&Weekday::Mon));
        assert!(rule.days_of_week.contains(&Weekday::Fri));
    }

    #[test]
    fn test_rule_with_priority() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024).with_priority(42);
        assert_eq!(rule.priority, 42);
    }

    #[test]
    fn test_rule_builder_chain() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 0, 17, 0, 1024 * 1024)
            .with_days(vec![Weekday::Mon])
            .with_priority(10);
        assert_eq!(rule.id, "1");
        assert_eq!(rule.priority, 10);
        assert_eq!(rule.days_of_week, vec![Weekday::Mon]);
        assert_eq!(rule.speed_limit_bps, 1024 * 1024);
    }

    // ===== BandwidthScheduleRule serde =====

    #[test]
    fn test_rule_serde_roundtrip() {
        let rule = BandwidthScheduleRule::new("r1", "Night Unlimited", 22, 0, 6, 0, 0)
            .with_days(vec![Weekday::Mon, Weekday::Fri])
            .with_priority(5);
        let json = serde_json::to_string(&rule).unwrap();
        let parsed: BandwidthScheduleRule = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "r1");
        assert_eq!(parsed.name, "Night Unlimited");
        assert_eq!(parsed.start_hour, 22);
        assert_eq!(parsed.end_hour, 6);
        assert_eq!(parsed.speed_limit_bps, 0);
        assert_eq!(parsed.priority, 5);
        assert_eq!(parsed.days_of_week.len(), 2);
        assert!(parsed.enabled);
    }

    #[test]
    fn test_rule_serde_extra_fields_ignored() {
        let json = r#"{
            "id": "r1",
            "name": "test",
            "start_hour": 9,
            "start_minute": 0,
            "end_hour": 17,
            "end_minute": 0,
            "speed_limit_bps": 1024,
            "days_of_week": [],
            "priority": 0,
            "enabled": true,
            "extra_field": "should be ignored"
        }"#;
        let parsed: BandwidthScheduleRule = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.id, "r1");
        assert_eq!(parsed.speed_limit_bps, 1024);
    }

    #[test]
    fn test_rule_serde_disabled() {
        let mut rule = BandwidthScheduleRule::new("r1", "test", 0, 0, 23, 59, 1024);
        rule.enabled = false;
        let json = serde_json::to_string(&rule).unwrap();
        let parsed: BandwidthScheduleRule = serde_json::from_str(&json).unwrap();
        assert!(!parsed.enabled);
    }

    // ===== BandwidthScheduleRule Clone/Debug =====

    #[test]
    fn test_rule_clone() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 0, 17, 0, 1024)
            .with_days(vec![Weekday::Mon])
            .with_priority(5);
        let cloned = rule.clone();
        assert_eq!(cloned.id, rule.id);
        assert_eq!(cloned.name, rule.name);
        assert_eq!(cloned.priority, rule.priority);
        assert_eq!(cloned.days_of_week, rule.days_of_week);
    }

    #[test]
    fn test_rule_debug() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 0, 17, 0, 1024);
        let debug = format!("{:?}", rule);
        assert!(debug.contains("BandwidthScheduleRule"));
        assert!(debug.contains("\"1\""));
        assert!(debug.contains("\"test\""));
    }

    // ===== matches_time boundaries =====

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

        // End of window (exclusive)
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

        // End of window (exclusive)
        let time = Local.with_ymd_and_hms(2026, 1, 2, 6, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_time_minute_precision() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 30, 17, 30, 1024);

        // Before start minute
        let time = Local.with_ymd_and_hms(2026, 1, 1, 9, 29, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // Exact start minute
        let time = Local.with_ymd_and_hms(2026, 1, 1, 9, 30, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Middle
        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 45, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Exact end minute (exclusive)
        let time = Local.with_ymd_and_hms(2026, 1, 1, 17, 30, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // One minute before end
        let time = Local.with_ymd_and_hms(2026, 1, 1, 17, 29, 0).unwrap();
        assert!(rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_time_full_day() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert!(rule.matches_time(&time));
        let time = Local.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert!(rule.matches_time(&time));
        let time = Local.with_ymd_and_hms(2026, 1, 1, 23, 58, 0).unwrap();
        assert!(rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_time_zero_duration() {
        // start == end means nothing matches (empty window)
        let rule = BandwidthScheduleRule::new("1", "test", 12, 0, 12, 0, 1024);
        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        // start_minutes == end_minutes, normal window: current >= start && current < end
        // 720 >= 720 && 720 < 720 => false
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

        // Thursday
        let time = Local.with_ymd_and_hms(2026, 1, 8, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // Friday
        let time = Local.with_ymd_and_hms(2026, 1, 9, 12, 0, 0).unwrap();
        assert!(rule.matches_time(&time));

        // Saturday
        let time = Local.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));

        // Sunday
        let time = Local.with_ymd_and_hms(2026, 1, 11, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_empty_days_means_every_day() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        assert!(rule.days_of_week.is_empty());
        // Should match any day
        for day in 5..12 {
            let time = Local.with_ymd_and_hms(2026, 1, day, 12, 0, 0).unwrap();
            assert!(rule.matches_time(&time));
        }
    }

    #[test]
    fn test_rule_disabled() {
        let mut rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        rule.enabled = false;

        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_rule_disabled_with_days() {
        let mut rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024)
            .with_days(vec![Weekday::Mon]);
        rule.enabled = false;

        // Monday
        let time = Local.with_ymd_and_hms(2026, 1, 5, 12, 0, 0).unwrap();
        assert!(!rule.matches_time(&time));
    }

    #[test]
    fn test_rule_matches_overnight_with_days() {
        // Overnight rule only on Fridays and Saturdays
        let rule = BandwidthScheduleRule::new("1", "test", 22, 0, 6, 0, 0)
            .with_days(vec![Weekday::Fri, Weekday::Sat]);

        // Friday night (Fri 23:00) - should match
        let time = Local.with_ymd_and_hms(2026, 1, 9, 23, 0, 0).unwrap(); // Friday
        assert!(rule.matches_time(&time));

        // Saturday early morning (Sat 02:00) - should match
        let time = Local.with_ymd_and_hms(2026, 1, 10, 2, 0, 0).unwrap(); // Saturday
        assert!(rule.matches_time(&time));

        // Monday night - should not match (wrong day)
        let time = Local.with_ymd_and_hms(2026, 1, 5, 23, 0, 0).unwrap(); // Monday
        assert!(!rule.matches_time(&time));
    }

    // ===== format_time_window =====

    #[test]
    fn test_rule_format_time_window() {
        let rule = BandwidthScheduleRule::new("1", "test", 9, 0, 17, 30, 1024);
        assert_eq!(rule.format_time_window(), "09:00-17:30");
    }

    #[test]
    fn test_rule_format_time_window_various() {
        assert_eq!(
            BandwidthScheduleRule::new("1", "t", 0, 0, 23, 59, 0).format_time_window(),
            "00:00-23:59"
        );
        assert_eq!(
            BandwidthScheduleRule::new("1", "t", 22, 30, 6, 15, 0).format_time_window(),
            "22:30-06:15"
        );
        assert_eq!(
            BandwidthScheduleRule::new("1", "t", 12, 0, 12, 0, 0).format_time_window(),
            "12:00-12:00"
        );
    }

    // ===== format_days =====

    #[test]
    fn test_rule_format_days() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        assert_eq!(rule.format_days(), "every day");

        let rule = rule.with_days(vec![Weekday::Mon, Weekday::Fri]);
        assert_eq!(rule.format_days(), "Mon, Fri");
    }

    #[test]
    fn test_rule_format_days_single() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024)
            .with_days(vec![Weekday::Sun]);
        assert_eq!(rule.format_days(), "Sun");
    }

    #[test]
    fn test_rule_format_days_all() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024).with_days(vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
            Weekday::Sat,
            Weekday::Sun,
        ]);
        assert_eq!(rule.format_days(), "Mon, Tue, Wed, Thu, Fri, Sat, Sun");
    }

    // ===== format_speed_limit =====

    #[test]
    fn test_rule_format_speed_limit() {
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 0);
        assert_eq!(rule.format_speed_limit(), "unlimited");

        let rule = BandwidthScheduleRule::new("2", "test", 0, 0, 23, 59, 5 * 1024 * 1024);
        assert_eq!(rule.format_speed_limit(), "5.0 MB/s");
    }

    #[test]
    fn test_rule_format_speed_limit_various() {
        assert_eq!(
            BandwidthScheduleRule::new("1", "t", 0, 0, 23, 59, 500).format_speed_limit(),
            "500 B/s"
        );
        assert_eq!(
            BandwidthScheduleRule::new("1", "t", 0, 0, 23, 59, 1024).format_speed_limit(),
            "1.0 KB/s"
        );
        assert_eq!(
            BandwidthScheduleRule::new("1", "t", 0, 0, 23, 59, 1024 * 1024 * 1024)
                .format_speed_limit(),
            "1.0 GB/s"
        );
    }

    // ===== BandwidthScheduleManager =====

    #[test]
    fn test_manager_new() {
        let manager = BandwidthScheduleManager::new();
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());
        assert!(manager.list_rules().is_empty());
    }

    #[test]
    fn test_manager_default() {
        let manager = BandwidthScheduleManager::default();
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());
    }

    #[test]
    fn test_manager_add_rule() {
        let mut manager = BandwidthScheduleManager::new();
        let rule = BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024);
        manager.add_rule(rule);
        assert_eq!(manager.len(), 1);
        assert!(!manager.is_empty());
    }

    #[test]
    fn test_manager_add_multiple_rules() {
        let mut manager = BandwidthScheduleManager::new();
        for i in 0..5 {
            let rule = BandwidthScheduleRule::new(
                format!("r{}", i),
                format!("rule {}", i),
                0,
                0,
                23,
                59,
                1024 * (i + 1) as u64,
            );
            manager.add_rule(rule);
        }
        assert_eq!(manager.len(), 5);
    }

    #[test]
    fn test_manager_get_rule() {
        let mut manager = BandwidthScheduleManager::new();
        let rule = BandwidthScheduleRule::new("r1", "test", 9, 0, 17, 0, 1024);
        manager.add_rule(rule);

        let found = manager.get_rule("r1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "test");

        assert!(manager.get_rule("nonexistent").is_none());
    }

    #[test]
    fn test_manager_list_rules() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "r1", "first", 0, 0, 23, 59, 1024,
        ));
        manager.add_rule(BandwidthScheduleRule::new(
            "r2", "second", 0, 0, 23, 59, 2048,
        ));

        let rules = manager.list_rules();
        assert_eq!(rules.len(), 2);
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
    fn test_manager_remove_idempotent() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024));
        assert!(manager.remove_rule("1"));
        assert!(!manager.remove_rule("1"));
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_manager_clear() {
        let mut manager = BandwidthScheduleManager::new();
        for i in 0..5 {
            manager.add_rule(BandwidthScheduleRule::new(
                format!("r{}", i),
                format!("rule {}", i),
                0,
                0,
                23,
                59,
                1024,
            ));
        }
        assert_eq!(manager.len(), 5);
        manager.clear();
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());
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
    fn test_manager_priority_ordering() {
        let mut manager = BandwidthScheduleManager::new();

        // Add rules in random priority order
        manager
            .add_rule(BandwidthScheduleRule::new("low", "l", 0, 0, 23, 59, 100).with_priority(1));
        manager
            .add_rule(BandwidthScheduleRule::new("high", "h", 0, 0, 23, 59, 300).with_priority(30));
        manager
            .add_rule(BandwidthScheduleRule::new("mid", "m", 0, 0, 23, 59, 200).with_priority(20));

        // list_rules should be sorted by priority descending
        let rules = manager.list_rules();
        assert_eq!(rules[0].id, "high");
        assert_eq!(rules[1].id, "mid");
        assert_eq!(rules[2].id, "low");
    }

    #[test]
    fn test_manager_same_priority_preserves_order() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new("a", "A", 0, 0, 23, 59, 100).with_priority(5));
        manager.add_rule(BandwidthScheduleRule::new("b", "B", 0, 0, 23, 59, 200).with_priority(5));
        // Both have same priority; sort is stable so order should be preserved
        let rules = manager.list_rules();
        assert_eq!(rules[0].id, "a");
        assert_eq!(rules[1].id, "b");
    }

    #[test]
    fn test_manager_negative_priority() {
        let mut manager = BandwidthScheduleManager::new();
        manager
            .add_rule(BandwidthScheduleRule::new("neg", "n", 0, 0, 23, 59, 100).with_priority(-1));
        manager
            .add_rule(BandwidthScheduleRule::new("zero", "z", 0, 0, 23, 59, 200).with_priority(0));

        let rules = manager.list_rules();
        assert_eq!(rules[0].id, "zero"); // higher priority first
        assert_eq!(rules[1].id, "neg");
    }

    #[test]
    fn test_manager_find_matching_rule_at() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "day",
            "Daytime",
            9,
            0,
            17,
            0,
            1024 * 1024,
        ));
        manager.add_rule(BandwidthScheduleRule::new(
            "night",
            "Nighttime",
            22,
            0,
            6,
            0,
            0,
        ));

        // During day
        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let matched = manager.find_matching_rule_at(&time);
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().id, "day");

        // During night
        let time = Local.with_ymd_and_hms(2026, 1, 1, 23, 0, 0).unwrap();
        let matched = manager.find_matching_rule_at(&time);
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().id, "night");

        // In between (no match)
        let time = Local.with_ymd_and_hms(2026, 1, 1, 18, 0, 0).unwrap();
        let matched = manager.find_matching_rule_at(&time);
        assert!(matched.is_none());
    }

    #[test]
    fn test_manager_find_matching_rule_at_priority() {
        let mut manager = BandwidthScheduleManager::new();
        // Two overlapping rules with different priorities
        manager.add_rule(
            BandwidthScheduleRule::new("low", "Low Priority", 0, 0, 23, 59, 1024).with_priority(1),
        );
        manager.add_rule(
            BandwidthScheduleRule::new("high", "High Priority", 0, 0, 23, 59, 1024 * 1024)
                .with_priority(10),
        );

        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let matched = manager.find_matching_rule_at(&time);
        assert_eq!(matched.unwrap().id, "high");
    }

    #[test]
    fn test_manager_speed_limit_at() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "day",
            "Day",
            9,
            0,
            17,
            0,
            1024 * 1024,
        ));

        let time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(manager.speed_limit_at(&time), Some(1024 * 1024));

        let time = Local.with_ymd_and_hms(2026, 1, 1, 20, 0, 0).unwrap();
        assert_eq!(manager.speed_limit_at(&time), None);
    }

    #[test]
    fn test_manager_speed_limit_at_unlimited() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new("night", "Night", 22, 0, 6, 0, 0));

        let time = Local.with_ymd_and_hms(2026, 1, 1, 23, 0, 0).unwrap();
        assert_eq!(manager.speed_limit_at(&time), Some(0));
    }

    #[test]
    fn test_manager_current_speed_limit() {
        let mut manager = BandwidthScheduleManager::new();
        // This test creates a rule that matches "now" by using a full-day window
        manager.add_rule(BandwidthScheduleRule::new(
            "always", "Always", 0, 0, 23, 59, 5000,
        ));
        assert_eq!(manager.current_speed_limit(), Some(5000));
    }

    #[test]
    fn test_manager_current_speed_limit_no_match() {
        let manager = BandwidthScheduleManager::new();
        assert_eq!(manager.current_speed_limit(), None);
    }

    // ===== BandwidthScheduleManager Unicode =====

    #[test]
    fn test_manager_unicode_ids() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "规则一",
            "中文规则",
            9,
            0,
            17,
            0,
            1024,
        ));
        manager.add_rule(BandwidthScheduleRule::new(
            "🚀fast", "Rocket", 0, 0, 23, 59, 0,
        ));

        assert!(manager.get_rule("规则一").is_some());
        assert!(manager.get_rule("🚀fast").is_some());
        assert_eq!(manager.len(), 2);
    }

    // ===== BandwidthScheduleManager Debug =====

    #[test]
    fn test_manager_debug() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new("1", "test", 0, 0, 23, 59, 1024));
        let debug = format!("{:?}", manager);
        assert!(debug.contains("BandwidthScheduleManager"));
    }

    // ===== Persistence =====

    #[tokio::test]
    async fn test_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(
            BandwidthScheduleRule::new("r1", "Night Unlimited", 22, 0, 6, 0, 0)
                .with_days(vec![Weekday::Fri, Weekday::Sat])
                .with_priority(5),
        );
        manager.add_rule(BandwidthScheduleRule::new(
            "r2",
            "Work Hours",
            9,
            0,
            17,
            0,
            1024 * 1024,
        ));

        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();

        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        // Rules should be sorted by priority
        assert_eq!(loaded.list_rules()[0].id, "r1"); // priority 5
        assert_eq!(loaded.list_rules()[1].id, "r2"); // priority 0
    }

    #[tokio::test]
    async fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let manager = BandwidthScheduleManager::new();
        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();
        assert!(dir.path().join("bandwidth_schedule.json").exists());
    }

    #[tokio::test]
    async fn test_load_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_bandwidth_schedule(dir.path()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bandwidth_schedule.json");
        std::fs::write(&path, "not valid json").unwrap();
        let result = load_bandwidth_schedule(dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_empty_rules() {
        let dir = tempfile::tempdir().unwrap();
        let manager = BandwidthScheduleManager::new();
        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();

        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 0);
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_save_overwrite() {
        let dir = tempfile::tempdir().unwrap();

        // Save initial
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "r1", "first", 0, 0, 23, 59, 1024,
        ));
        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();

        // Overwrite with different data
        let mut manager2 = BandwidthScheduleManager::new();
        manager2.add_rule(BandwidthScheduleRule::new(
            "r2", "second", 9, 0, 17, 0, 2048,
        ));
        manager2.add_rule(BandwidthScheduleRule::new("r3", "third", 22, 0, 6, 0, 0));
        save_bandwidth_schedule(&manager2, dir.path())
            .await
            .unwrap();

        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.get_rule("r1").is_none());
        assert!(loaded.get_rule("r2").is_some());
    }

    #[tokio::test]
    async fn test_save_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new("r1", "test", 9, 0, 17, 0, 1024));
        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();

        let content = std::fs::read_to_string(dir.path().join("bandwidth_schedule.json")).unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    #[tokio::test]
    async fn test_save_load_preserves_all_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(
            BandwidthScheduleRule::new("full", "Full Rule", 10, 30, 18, 45, 5 * 1024 * 1024)
                .with_days(vec![Weekday::Mon, Weekday::Wed, Weekday::Fri])
                .with_priority(42),
        );
        // Disable the rule
        manager.list_rules_mut().first_mut().unwrap().enabled = false;

        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();
        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        let rule = loaded.get_rule("full").unwrap();
        assert_eq!(rule.name, "Full Rule");
        assert_eq!(rule.start_hour, 10);
        assert_eq!(rule.start_minute, 30);
        assert_eq!(rule.end_hour, 18);
        assert_eq!(rule.end_minute, 45);
        assert_eq!(rule.speed_limit_bps, 5 * 1024 * 1024);
        assert_eq!(rule.priority, 42);
        assert_eq!(rule.days_of_week.len(), 3);
        assert!(!rule.enabled);
    }

    // ===== BandwidthScheduleError =====

    #[test]
    fn test_error_display_io() {
        let err = BandwidthScheduleError::Io("disk full".to_string());
        assert_eq!(format!("{}", err), "IO error: disk full");
    }

    #[test]
    fn test_error_display_serialize() {
        let err = BandwidthScheduleError::Serialize("invalid data".to_string());
        assert_eq!(format!("{}", err), "Serialization error: invalid data");
    }

    #[test]
    fn test_error_display_deserialize() {
        let err = BandwidthScheduleError::Deserialize("unexpected token".to_string());
        assert_eq!(
            format!("{}", err),
            "Deserialization error: unexpected token"
        );
    }

    #[test]
    fn test_error_debug() {
        let err = BandwidthScheduleError::Io("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
        assert!(debug.contains("test"));
    }

    // ===== Complex workflows =====

    #[tokio::test]
    async fn test_full_lifecycle() {
        let dir = tempfile::tempdir().unwrap();

        // Create manager with rules
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(
            BandwidthScheduleRule::new("work", "Work Hours", 9, 0, 17, 0, 1024 * 1024)
                .with_days(vec![
                    Weekday::Mon,
                    Weekday::Tue,
                    Weekday::Wed,
                    Weekday::Thu,
                    Weekday::Fri,
                ])
                .with_priority(10),
        );
        manager.add_rule(
            BandwidthScheduleRule::new("night", "Night Unlimited", 22, 0, 6, 0, 0).with_priority(5),
        );

        // Verify rules
        assert_eq!(manager.len(), 2);
        assert_eq!(manager.list_rules()[0].id, "work"); // higher priority first

        // Save
        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();

        // Load and verify
        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.list_rules()[0].id, "work");

        // Evaluate at specific times
        let work_time = Local.with_ymd_and_hms(2026, 1, 5, 12, 0, 0).unwrap(); // Monday noon
        assert_eq!(loaded.speed_limit_at(&work_time), Some(1024 * 1024));

        let night_time = Local.with_ymd_and_hms(2026, 1, 5, 23, 0, 0).unwrap(); // Monday night
        assert_eq!(loaded.speed_limit_at(&night_time), Some(0));

        let free_time = Local.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap(); // Saturday noon
        assert_eq!(loaded.speed_limit_at(&free_time), None); // no rule matches
    }

    #[test]
    fn test_multiple_overlapping_rules() {
        let mut manager = BandwidthScheduleManager::new();

        // General slow all day
        manager.add_rule(
            BandwidthScheduleRule::new("slow", "Slow All Day", 0, 0, 23, 59, 512 * 1024)
                .with_priority(1),
        );
        // Faster during work hours
        manager.add_rule(
            BandwidthScheduleRule::new("work", "Work Fast", 9, 0, 17, 0, 5 * 1024 * 1024)
                .with_priority(10),
        );
        // Unlimited at night
        manager.add_rule(
            BandwidthScheduleRule::new("night", "Night Unlimited", 22, 0, 6, 0, 0)
                .with_priority(20),
        );

        // Night has highest priority
        let night_time = Local.with_ymd_and_hms(2026, 1, 1, 23, 0, 0).unwrap();
        assert_eq!(manager.speed_limit_at(&night_time), Some(0));

        // Work hours
        let work_time = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(manager.speed_limit_at(&work_time), Some(5 * 1024 * 1024));

        // Evening (18:00-21:59) - only slow rule matches
        let evening_time = Local.with_ymd_and_hms(2026, 1, 1, 19, 0, 0).unwrap();
        assert_eq!(manager.speed_limit_at(&evening_time), Some(512 * 1024));
    }

    #[test]
    fn test_add_remove_add_cycle() {
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "r1", "first", 0, 0, 23, 59, 1024,
        ));
        assert_eq!(manager.len(), 1);

        manager.remove_rule("r1");
        assert_eq!(manager.len(), 0);

        manager.add_rule(BandwidthScheduleRule::new(
            "r1", "re-added", 9, 0, 17, 0, 2048,
        ));
        assert_eq!(manager.len(), 1);
        assert_eq!(manager.get_rule("r1").unwrap().name, "re-added");
    }

    // ===== Serde for rules list =====

    #[test]
    fn test_rules_vec_serde() {
        let rules = vec![
            BandwidthScheduleRule::new("r1", "first", 9, 0, 17, 0, 1024),
            BandwidthScheduleRule::new("r2", "second", 22, 0, 6, 0, 0),
        ];
        let json = serde_json::to_string(&rules).unwrap();
        let parsed: Vec<BandwidthScheduleRule> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].id, "r1");
        assert_eq!(parsed[1].id, "r2");
    }

    // ===== Edge cases =====

    #[test]
    fn test_rule_zero_speed_limit() {
        let rule = BandwidthScheduleRule::new("1", "unlimited", 0, 0, 23, 59, 0);
        assert_eq!(rule.speed_limit_bps, 0);
        assert_eq!(rule.format_speed_limit(), "unlimited");
    }

    #[test]
    fn test_rule_very_large_speed_limit() {
        let rule = BandwidthScheduleRule::new("1", "fast", 0, 0, 23, 59, u64::MAX);
        assert_eq!(rule.speed_limit_bps, u64::MAX);
    }

    #[tokio::test]
    async fn test_persistence_unicode_rule_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = BandwidthScheduleManager::new();
        manager.add_rule(BandwidthScheduleRule::new(
            "中文规则",
            "工作时间限速",
            9,
            0,
            17,
            0,
            1024 * 1024,
        ));
        manager.add_rule(BandwidthScheduleRule::new(
            "🚀规则",
            "🌙夜间无限",
            22,
            0,
            6,
            0,
            0,
        ));

        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();
        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.get_rule("中文规则").is_some());
        assert!(loaded.get_rule("🚀规则").is_some());
    }

    #[tokio::test]
    async fn test_persistence_empty_manager() {
        let dir = tempfile::tempdir().unwrap();
        let manager = BandwidthScheduleManager::new();
        save_bandwidth_schedule(&manager, dir.path()).await.unwrap();

        let loaded = load_bandwidth_schedule(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn test_manager_independence() {
        let mut m1 = BandwidthScheduleManager::new();
        let mut m2 = BandwidthScheduleManager::new();

        m1.add_rule(BandwidthScheduleRule::new(
            "r1",
            "only in m1",
            0,
            0,
            23,
            59,
            1024,
        ));
        assert_eq!(m1.len(), 1);
        assert_eq!(m2.len(), 0);
    }
}
