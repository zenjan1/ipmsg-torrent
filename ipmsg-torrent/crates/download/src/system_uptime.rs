//! System Uptime Tracker - Tracks how long the download manager has been running
//!
//! Provides uptime tracking for the dashboard and monitoring, replacing the
//! hardcoded `uptime_seconds: 0` placeholder.

use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Tracks system uptime since DownloadManager initialization
#[derive(Debug, Clone)]
pub struct SystemUptimeTracker {
    /// The instant when the system started
    started_at: Instant,
    /// The timestamp when the system started (for persistence/display)
    started_at_timestamp: chrono::DateTime<chrono::Utc>,
}

impl SystemUptimeTracker {
    /// Create a new uptime tracker, recording the current time as start
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            started_at_timestamp: chrono::Utc::now(),
        }
    }

    /// Create from a known start timestamp (for restored state)
    pub fn from_timestamp(started_at: chrono::DateTime<chrono::Utc>) -> Self {
        // Calculate elapsed duration and create Instant in the past
        let elapsed = chrono::Utc::now()
            .signed_duration_since(started_at)
            .to_std()
            .unwrap_or_default();
        let started_at_instant = Instant::now() - elapsed;

        Self {
            started_at: started_at_instant,
            started_at_timestamp: started_at,
        }
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Get uptime in milliseconds (for more precise tracking)
    pub fn uptime_millis(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }

    /// Get the timestamp when the system started
    pub fn started_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.started_at_timestamp
    }

    /// Format uptime as human-readable string (e.g., "2h 34m 56s")
    pub fn format_uptime(&self) -> String {
        let secs = self.uptime_seconds();
        format_duration(secs)
    }

    /// Get uptime summary for display
    pub fn summary(&self) -> UptimeSummary {
        let secs = self.uptime_seconds();
        UptimeSummary {
            uptime_seconds: secs,
            uptime_formatted: format_duration(secs),
            started_at: self.started_at_timestamp,
        }
    }
}

impl Default for SystemUptimeTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of system uptime for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UptimeSummary {
    /// Uptime in seconds
    pub uptime_seconds: u64,
    /// Human-readable formatted uptime
    pub uptime_formatted: String,
    /// Timestamp when the system started
    pub started_at: chrono::DateTime<chrono::Utc>,
}

/// Format seconds into human-readable duration
pub fn format_duration(secs: u64) -> String {
    if secs == 0 {
        return "0s".to_string();
    }

    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}m", minutes));
    }
    if seconds > 0 || parts.is_empty() {
        parts.push(format!("{}s", seconds));
    }

    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_tracker_starts_at_zero() {
        let tracker = SystemUptimeTracker::new();
        // Should be very close to 0 (allow some margin for test execution)
        assert!(tracker.uptime_seconds() < 2);
    }

    #[test]
    fn test_uptime_increases() {
        let tracker = SystemUptimeTracker::new();
        thread::sleep(Duration::from_millis(100));
        let uptime = tracker.uptime_seconds();
        // Should be at least 0, could be 0 or 1 depending on timing
        assert!(uptime < 5);
    }

    #[test]
    fn test_uptime_millis_precision() {
        let tracker = SystemUptimeTracker::new();
        thread::sleep(Duration::from_millis(50));
        let millis = tracker.uptime_millis();
        assert!(millis >= 40); // Allow some margin
        assert!(millis < 1000);
    }

    #[test]
    fn test_started_at_timestamp() {
        let before = chrono::Utc::now();
        let tracker = SystemUptimeTracker::new();
        let after = chrono::Utc::now();

        let started = tracker.started_at();
        assert!(started >= before - chrono::Duration::seconds(1));
        assert!(started <= after + chrono::Duration::seconds(1));
    }

    #[test]
    fn test_from_timestamp_past() {
        // Create tracker with start time 1 hour ago
        let one_hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        let tracker = SystemUptimeTracker::from_timestamp(one_hour_ago);

        let uptime = tracker.uptime_seconds();
        // Should be approximately 3600 seconds (1 hour)
        assert!(uptime >= 3590 && uptime <= 3610);
    }

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn test_format_duration_seconds_only() {
        assert_eq!(format_duration(45), "45s");
    }

    #[test]
    fn test_format_duration_minutes_seconds() {
        assert_eq!(format_duration(125), "2m 5s");
    }

    #[test]
    fn test_format_duration_hours_minutes_seconds() {
        assert_eq!(format_duration(3661), "1h 1m 1s");
    }

    #[test]
    fn test_format_duration_days() {
        assert_eq!(format_duration(90061), "1d 1h 1m 1s");
    }

    #[test]
    fn test_format_duration_exact_hour() {
        assert_eq!(format_duration(3600), "1h");
    }

    #[test]
    fn test_format_duration_exact_day() {
        assert_eq!(format_duration(86400), "1d");
    }

    #[test]
    fn test_summary_structure() {
        let tracker = SystemUptimeTracker::new();
        let summary = tracker.summary();

        assert!(summary.uptime_seconds < 5);
        assert!(!summary.uptime_formatted.is_empty());
        assert!(summary.started_at <= chrono::Utc::now());
    }

    #[test]
    fn test_summary_serialization() {
        let tracker = SystemUptimeTracker::new();
        let summary = tracker.summary();

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: UptimeSummary = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.uptime_seconds, summary.uptime_seconds);
        assert_eq!(deserialized.uptime_formatted, summary.uptime_formatted);
    }

    #[test]
    fn test_default_impl() {
        let tracker = SystemUptimeTracker::default();
        assert!(tracker.uptime_seconds() < 2);
    }

    #[test]
    fn test_format_uptime_method() {
        let tracker = SystemUptimeTracker::new();
        let formatted = tracker.format_uptime();
        // Should be "0s" or "1s" depending on timing
        assert!(formatted == "0s" || formatted == "1s");
    }

    // ===== Phase 231: Comprehensive test coverage =====

    // ── format_duration boundary tests ──

    #[test]
    fn test_format_duration_one_second() {
        assert_eq!(format_duration(1), "1s");
    }

    #[test]
    fn test_format_duration_59_seconds() {
        assert_eq!(format_duration(59), "59s");
    }

    #[test]
    fn test_format_duration_exact_minute() {
        assert_eq!(format_duration(60), "1m");
    }

    #[test]
    fn test_format_duration_61_seconds() {
        assert_eq!(format_duration(61), "1m 1s");
    }

    #[test]
    fn test_format_duration_59_minutes_59_seconds() {
        assert_eq!(format_duration(3599), "59m 59s");
    }

    #[test]
    fn test_format_duration_exact_2_hours() {
        assert_eq!(format_duration(7200), "2h");
    }

    #[test]
    fn test_format_duration_23h_59m_59s() {
        assert_eq!(format_duration(86399), "23h 59m 59s");
    }

    #[test]
    fn test_format_duration_exact_2_days() {
        assert_eq!(format_duration(172800), "2d");
    }

    #[test]
    fn test_format_duration_large_value() {
        // 365 days + 5 hours + 30 minutes + 15 seconds
        let secs = 365 * 86400 + 5 * 3600 + 30 * 60 + 15;
        assert_eq!(format_duration(secs), "365d 5h 30m 15s");
    }

    #[test]
    fn test_format_duration_very_large_value() {
        // 1000 days
        assert_eq!(format_duration(86_400_000), "1000d");
    }

    #[test]
    fn test_format_duration_u64_max() {
        // Should not panic
        let result = format_duration(u64::MAX);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_format_duration_zero_minutes_no_zero_seconds() {
        // 1h exactly: should be "1h", not "1h 0m 0s"
        assert_eq!(format_duration(3600), "1h");
    }

    #[test]
    fn test_format_duration_zero_hours_no_zero_minutes() {
        // 1d exactly: should be "1d", not "1d 0h 0m 0s"
        assert_eq!(format_duration(86400), "1d");
    }

    #[test]
    fn test_format_duration_days_and_seconds_no_hours_minutes() {
        // 1 day + 30 seconds = "1d 30s"
        assert_eq!(format_duration(86430), "1d 30s");
    }

    // ── UptimeSummary serde tests ──

    #[test]
    fn test_uptime_summary_serde_roundtrip() {
        let summary = UptimeSummary {
            uptime_seconds: 12345,
            uptime_formatted: "3h 25m 45s".to_string(),
            started_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: UptimeSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.uptime_seconds, 12345);
        assert_eq!(deserialized.uptime_formatted, "3h 25m 45s");
    }

    #[test]
    fn test_uptime_summary_serde_zero_values() {
        let summary = UptimeSummary {
            uptime_seconds: 0,
            uptime_formatted: "0s".to_string(),
            started_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: UptimeSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.uptime_seconds, 0);
        assert_eq!(deserialized.uptime_formatted, "0s");
    }

    #[test]
    fn test_uptime_summary_serde_large_uptime() {
        let summary = UptimeSummary {
            uptime_seconds: u64::MAX,
            uptime_formatted: "very long".to_string(),
            started_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: UptimeSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.uptime_seconds, u64::MAX);
    }

    #[test]
    fn test_uptime_summary_serde_extra_fields_ignored() {
        let json = r#"{"uptime_seconds":10,"uptime_formatted":"10s","started_at":"2026-01-01T00:00:00Z","extra_field":"ignored"}"#;
        let deserialized: UptimeSummary = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.uptime_seconds, 10);
        assert_eq!(deserialized.uptime_formatted, "10s");
    }

    #[test]
    fn test_uptime_summary_serde_pretty() {
        let summary = UptimeSummary {
            uptime_seconds: 3661,
            uptime_formatted: "1h 1m 1s".to_string(),
            started_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        };
        let pretty = serde_json::to_string_pretty(&summary).unwrap();
        assert!(pretty.contains("\"uptime_seconds\""));
        assert!(pretty.contains("\"uptime_formatted\""));
        let deserialized: UptimeSummary = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.uptime_seconds, 3661);
    }

    // ── Clone trait tests ──

    #[test]
    fn test_clone_tracker() {
        let tracker = SystemUptimeTracker::new();
        let cloned = tracker.clone();
        // Both should report similar uptime
        assert!(cloned.uptime_seconds() < 2);
    }

    #[test]
    fn test_clone_independence() {
        let tracker1 = SystemUptimeTracker::new();
        let tracker2 = tracker1.clone();
        // Both started at the same time
        assert_eq!(
            tracker1.started_at().timestamp_millis(),
            tracker2.started_at().timestamp_millis()
        );
    }

    #[test]
    fn test_clone_summary() {
        let summary = UptimeSummary {
            uptime_seconds: 100,
            uptime_formatted: "1m 40s".to_string(),
            started_at: chrono::Utc::now(),
        };
        let cloned = summary.clone();
        assert_eq!(cloned.uptime_seconds, 100);
        assert_eq!(cloned.uptime_formatted, "1m 40s");
    }

    // ── Debug trait tests ──

    #[test]
    fn test_debug_tracker() {
        let tracker = SystemUptimeTracker::new();
        let debug_str = format!("{:?}", tracker);
        assert!(debug_str.contains("SystemUptimeTracker"));
    }

    #[test]
    fn test_debug_summary() {
        let summary = UptimeSummary {
            uptime_seconds: 42,
            uptime_formatted: "42s".to_string(),
            started_at: chrono::Utc::now(),
        };
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("UptimeSummary"));
        assert!(debug_str.contains("42"));
    }

    // ── from_timestamp edge cases ──

    #[test]
    fn test_from_timestamp_now() {
        let now = chrono::Utc::now();
        let tracker = SystemUptimeTracker::from_timestamp(now);
        assert!(tracker.uptime_seconds() < 2);
    }

    #[test]
    fn test_from_timestamp_very_old() {
        // Start time 30 days ago
        let thirty_days_ago = chrono::Utc::now() - chrono::Duration::days(30);
        let tracker = SystemUptimeTracker::from_timestamp(thirty_days_ago);
        let uptime = tracker.uptime_seconds();
        // Should be approximately 30 * 86400 = 2_592_000 seconds
        assert!(uptime >= 2_590_000 && uptime <= 2_600_000);
    }

    #[test]
    fn test_from_timestamp_preserves_original_timestamp() {
        let specific_time = chrono::DateTime::parse_from_rfc3339("2025-06-15T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let tracker = SystemUptimeTracker::from_timestamp(specific_time);
        assert_eq!(tracker.started_at(), specific_time);
    }

    #[test]
    fn test_from_timestamp_uptime_grows() {
        let ten_secs_ago = chrono::Utc::now() - chrono::Duration::seconds(10);
        let tracker = SystemUptimeTracker::from_timestamp(ten_secs_ago);
        let uptime1 = tracker.uptime_seconds();
        thread::sleep(Duration::from_millis(100));
        let uptime2 = tracker.uptime_seconds();
        assert!(uptime2 >= uptime1);
    }

    // ── Uptime accessor consistency ──

    #[test]
    fn test_uptime_millis_greater_than_seconds() {
        let tracker = SystemUptimeTracker::new();
        thread::sleep(Duration::from_millis(50));
        let secs = tracker.uptime_seconds();
        let millis = tracker.uptime_millis();
        // millis should be >= secs * 1000 (approximately)
        assert!(millis >= (secs as u128) * 1000);
    }

    #[test]
    fn test_started_at_consistent_across_calls() {
        let tracker = SystemUptimeTracker::new();
        let ts1 = tracker.started_at();
        let ts2 = tracker.started_at();
        assert_eq!(ts1, ts2);
    }

    #[test]
    fn test_format_uptime_matches_summary() {
        let tracker = SystemUptimeTracker::new();
        let formatted = tracker.format_uptime();
        let summary = tracker.summary();
        assert_eq!(formatted, summary.uptime_formatted);
    }

    // ── Summary field correctness ──

    #[test]
    fn test_summary_started_at_matches_tracker() {
        let tracker = SystemUptimeTracker::new();
        let summary = tracker.summary();
        assert_eq!(summary.started_at, tracker.started_at());
    }

    #[test]
    fn test_summary_uptime_seconds_matches_tracker() {
        let tracker = SystemUptimeTracker::new();
        let summary = tracker.summary();
        // Allow 1 second margin for timing
        let diff = (summary.uptime_seconds as i64 - tracker.uptime_seconds() as i64).unsigned_abs();
        assert!(diff <= 1);
    }

    #[test]
    fn test_summary_from_timestamp_tracker() {
        let one_hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        let tracker = SystemUptimeTracker::from_timestamp(one_hour_ago);
        let summary = tracker.summary();
        assert_eq!(summary.started_at, one_hour_ago);
        assert!(summary.uptime_seconds >= 3590 && summary.uptime_seconds <= 3610);
    }

    // ── Default == new ──

    #[test]
    fn test_default_equals_new() {
        let default_tracker = SystemUptimeTracker::default();
        let new_tracker = SystemUptimeTracker::new();
        // Both should have very similar uptime
        assert!(default_tracker.uptime_seconds() < 2);
        assert!(new_tracker.uptime_seconds() < 2);
    }

    // ── Multiple trackers independent ──

    #[test]
    fn test_multiple_trackers_independent() {
        let tracker1 = SystemUptimeTracker::new();
        thread::sleep(Duration::from_millis(50));
        let tracker2 = SystemUptimeTracker::new();
        // tracker1 should have been running longer
        assert!(tracker1.uptime_millis() >= tracker2.uptime_millis());
    }

    #[test]
    fn test_multiple_trackers_different_timestamps() {
        let t1 =
            SystemUptimeTracker::from_timestamp(chrono::Utc::now() - chrono::Duration::hours(2));
        let t2 =
            SystemUptimeTracker::from_timestamp(chrono::Utc::now() - chrono::Duration::hours(1));
        // t1 should have more uptime
        assert!(t1.uptime_seconds() > t2.uptime_seconds());
    }

    // ── format_duration component isolation ──

    #[test]
    fn test_format_duration_only_minutes() {
        assert_eq!(format_duration(300), "5m");
    }

    #[test]
    fn test_format_duration_days_hours() {
        // 1 day + 1 hour = 90000 seconds
        assert_eq!(format_duration(90000), "1d 1h");
    }

    #[test]
    fn test_format_duration_days_minutes() {
        // 1 day + 1 minute = 86460 seconds
        assert_eq!(format_duration(86460), "1d 1m");
    }

    #[test]
    fn test_format_duration_hours_seconds() {
        // 1 hour + 1 second = 3601
        assert_eq!(format_duration(3601), "1h 1s");
    }

    // ── UptimeSummary JSON structure ──

    #[test]
    fn test_uptime_summary_json_contains_all_fields() {
        let summary = UptimeSummary {
            uptime_seconds: 100,
            uptime_formatted: "1m 40s".to_string(),
            started_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"uptime_seconds\""));
        assert!(json.contains("\"uptime_formatted\""));
        assert!(json.contains("\"started_at\""));
        assert!(json.contains("100"));
        assert!(json.contains("1m 40s"));
    }
}
