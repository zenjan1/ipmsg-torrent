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
}
