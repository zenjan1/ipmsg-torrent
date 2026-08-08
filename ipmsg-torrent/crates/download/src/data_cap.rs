//! Daily Data Cap with Auto-Pause
//!
//! Tracks daily download data usage and automatically pauses all active
//! downloads when a configured daily limit is reached. Resets at midnight.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Configuration for daily data cap
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataCapConfig {
    /// Whether the data cap is enabled
    pub enabled: bool,
    /// Daily data cap in bytes (0 = unlimited)
    pub daily_limit_bytes: u64,
}

impl DataCapConfig {
    pub fn new(enabled: bool, daily_limit_bytes: u64) -> Self {
        Self {
            enabled,
            daily_limit_bytes,
        }
    }
}

/// Tracks data usage for a single calendar day
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyUsage {
    /// The date this usage is for (UTC)
    pub date: NaiveDate,
    /// Total bytes downloaded on this date
    pub bytes_downloaded: u64,
    /// Number of download tasks that contributed to this usage
    pub task_count: u32,
    /// Last time this record was updated
    pub last_updated: DateTime<Utc>,
}

impl DailyUsage {
    pub fn new(date: NaiveDate) -> Self {
        Self {
            date,
            bytes_downloaded: 0,
            task_count: 0,
            last_updated: Utc::now(),
        }
    }

    /// Record a download increment
    pub fn add_bytes(&mut self, bytes: u64) {
        self.bytes_downloaded += bytes;
        self.last_updated = Utc::now();
    }

    /// Increment the task count (called once per task per day)
    pub fn increment_task_count(&mut self) {
        self.task_count += 1;
        self.last_updated = Utc::now();
    }

    /// Check if this usage exceeds the given limit
    pub fn exceeds(&self, limit_bytes: u64) -> bool {
        limit_bytes > 0 && self.bytes_downloaded >= limit_bytes
    }

    /// Get remaining bytes before hitting the limit
    pub fn remaining(&self, limit_bytes: u64) -> u64 {
        if limit_bytes == 0 {
            return u64::MAX;
        }
        limit_bytes.saturating_sub(self.bytes_downloaded)
    }

    /// Get usage as a percentage of the limit
    pub fn usage_percent(&self, limit_bytes: u64) -> f64 {
        if limit_bytes == 0 {
            return 0.0;
        }
        (self.bytes_downloaded as f64 / limit_bytes as f64) * 100.0
    }
}

/// Summary of data cap status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCapStatus {
    /// Current configuration
    pub config: DataCapConfig,
    /// Today's usage
    pub today_usage: DailyUsage,
    /// Whether the cap has been reached today
    pub cap_reached: bool,
    /// Whether downloads should be paused
    pub should_pause: bool,
    /// Bytes remaining today (u64::MAX if unlimited)
    pub remaining_bytes: u64,
    /// Usage percentage (0-100+)
    pub usage_percent: f64,
}

impl DataCapStatus {
    pub fn format_display(&self) -> String {
        let mut output = String::from("📊 Daily Data Cap Status\n");

        if !self.config.enabled {
            output.push_str("  Status: Disabled\n");
            return output;
        }

        let limit_str = format_bytes(self.config.daily_limit_bytes);
        let used_str = format_bytes(self.today_usage.bytes_downloaded);
        let remaining_str = if self.remaining_bytes == u64::MAX {
            "∞".to_string()
        } else {
            format_bytes(self.remaining_bytes)
        };

        output.push_str(&format!(
            "  Status: {}\n",
            if self.cap_reached {
                "🔴 CAP REACHED"
            } else {
                "🟢 Active"
            }
        ));
        output.push_str(&format!("  Daily limit: {}\n", limit_str));
        output.push_str(&format!(
            "  Used today: {} ({:.1}%)\n",
            used_str, self.usage_percent
        ));
        output.push_str(&format!("  Remaining: {}\n", remaining_str));
        output.push_str(&format!("  Tasks today: {}\n", self.today_usage.task_count));

        if self.cap_reached {
            output.push_str("  ⚠️  Downloads will be paused until tomorrow\n");
        }

        output
    }
}

/// Manager for daily data cap tracking
#[derive(Debug, Clone)]
pub struct DataCapManager {
    config: DataCapConfig,
    current_usage: DailyUsage,
    /// Tasks that have already been counted today (to avoid double-counting)
    counted_tasks: std::collections::HashSet<String>,
    /// Whether downloads were auto-paused due to cap
    auto_paused: bool,
}

impl Default for DataCapManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DataCapManager {
    pub fn new() -> Self {
        Self {
            config: DataCapConfig::default(),
            current_usage: DailyUsage::new(Utc::now().date_naive()),
            counted_tasks: std::collections::HashSet::new(),
            auto_paused: false,
        }
    }

    /// Set the data cap configuration
    pub fn set_config(&mut self, config: DataCapConfig) {
        self.config = config;
    }

    /// Get the current configuration
    pub fn config(&self) -> &DataCapConfig {
        &self.config
    }

    /// Enable or disable the data cap
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Set the daily limit in bytes
    pub fn set_daily_limit(&mut self, bytes: u64) {
        self.config.daily_limit_bytes = bytes;
    }

    /// Get current status
    pub fn status(&self) -> DataCapStatus {
        let cap_reached = self.is_cap_reached();
        let should_pause = cap_reached && self.config.enabled;
        let remaining_bytes = self.current_usage.remaining(self.config.daily_limit_bytes);
        let usage_percent = self
            .current_usage
            .usage_percent(self.config.daily_limit_bytes);

        DataCapStatus {
            config: self.config.clone(),
            today_usage: self.current_usage.clone(),
            cap_reached,
            should_pause,
            remaining_bytes,
            usage_percent,
        }
    }

    /// Check if the daily cap has been reached
    pub fn is_cap_reached(&self) -> bool {
        if !self.config.enabled || self.config.daily_limit_bytes == 0 {
            return false;
        }
        self.current_usage.exceeds(self.config.daily_limit_bytes)
    }

    /// Check if downloads should be paused (cap reached and not yet paused)
    pub fn should_pause_downloads(&self) -> bool {
        self.is_cap_reached() && !self.auto_paused
    }

    /// Mark that downloads have been auto-paused
    pub fn mark_auto_paused(&mut self) {
        self.auto_paused = true;
    }

    /// Reset the auto-paused flag (e.g., after midnight reset or manual resume)
    pub fn clear_auto_paused(&mut self) {
        self.auto_paused = false;
    }

    /// Whether downloads were auto-paused due to cap
    pub fn is_auto_paused(&self) -> bool {
        self.auto_paused
    }

    /// Record bytes downloaded for a task
    /// Returns true if this caused the cap to be reached
    pub fn record_download(&mut self, task_id: &str, bytes: u64) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Check for day rollover
        self.check_day_rollover();

        let was_below_cap = !self.current_usage.exceeds(self.config.daily_limit_bytes);

        self.current_usage.add_bytes(bytes);

        // Count task once per day
        if !self.counted_tasks.contains(task_id) {
            self.counted_tasks.insert(task_id.to_string());
            self.current_usage.increment_task_count();
        }

        let now_above_cap = self.current_usage.exceeds(self.config.daily_limit_bytes);

        // Return true if we just crossed the cap
        was_below_cap && now_above_cap
    }

    /// Check if we need to roll over to a new day
    pub fn check_day_rollover(&mut self) {
        let today = Utc::now().date_naive();
        if self.current_usage.date != today {
            self.current_usage = DailyUsage::new(today);
            self.counted_tasks.clear();
            self.auto_paused = false;
        }
    }

    /// Get today's usage
    pub fn today_usage(&self) -> &DailyUsage {
        &self.current_usage
    }

    /// Get bytes remaining today
    pub fn remaining_bytes(&self) -> u64 {
        self.current_usage.remaining(self.config.daily_limit_bytes)
    }

    /// Reset today's usage (for testing or manual reset)
    pub fn reset_today(&mut self) {
        self.current_usage = DailyUsage::new(Utc::now().date_naive());
        self.counted_tasks.clear();
        self.auto_paused = false;
    }
}

/// Format bytes into human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Parse a human-readable size string into bytes
/// Supports: "1GB", "500MB", "100KB", "1TB", "1024" (plain bytes)
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Try plain number first
    if let Ok(bytes) = s.parse::<u64>() {
        return Some(bytes);
    }

    // Find where the number ends and unit begins
    let (num_str, unit) = s
        .char_indices()
        .find(|(_, c)| c.is_alphabetic())
        .map(|(idx, _)| (&s[..idx], &s[idx..]))
        .unwrap_or((s, ""));

    let num: f64 = num_str.trim().parse().ok()?;
    if num < 0.0 {
        return None;
    }

    let multiplier = match unit.to_uppercase().as_str() {
        "B" => 1u64,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        "T" | "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return None,
    };

    Some((num * multiplier as f64) as u64)
}

/// Save data cap config and usage to disk
pub fn save_data_cap(manager: &DataCapManager, data_dir: &Path) -> Result<(), std::io::Error> {
    let path = data_dir.join("data_cap.json");

    let data = DataCapPersistData {
        config: manager.config.clone(),
        usage: manager.current_usage.clone(),
        auto_paused: manager.auto_paused,
    };

    let json = serde_json::to_string_pretty(&data).map_err(std::io::Error::other)?;

    // Atomic write
    let temp_path = path.with_extension("json.tmp");
    std::fs::write(&temp_path, json)?;
    std::fs::rename(&temp_path, &path)?;
    Ok(())
}

/// Data structure for persistence
#[derive(Debug, Serialize, Deserialize)]
struct DataCapPersistData {
    config: DataCapConfig,
    usage: DailyUsage,
    auto_paused: bool,
}

/// Load data cap config and usage from disk
pub fn load_data_cap(data_dir: &Path) -> Option<DataCapManager> {
    let path = data_dir.join("data_cap.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let data: DataCapPersistData = serde_json::from_str(&json).ok()?;

    let mut manager = DataCapManager::new();
    manager.config = data.config;
    manager.current_usage = data.usage;
    manager.auto_paused = data.auto_paused;

    // Check for day rollover on load
    manager.check_day_rollover();

    Some(manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_cap_config_default() {
        let config = DataCapConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.daily_limit_bytes, 0);
    }

    #[test]
    fn test_daily_usage_new() {
        let today = Utc::now().date_naive();
        let usage = DailyUsage::new(today);
        assert_eq!(usage.date, today);
        assert_eq!(usage.bytes_downloaded, 0);
        assert_eq!(usage.task_count, 0);
    }

    #[test]
    fn test_daily_usage_add_bytes() {
        let today = Utc::now().date_naive();
        let mut usage = DailyUsage::new(today);
        usage.add_bytes(1024);
        usage.add_bytes(2048);
        assert_eq!(usage.bytes_downloaded, 3072);
    }

    #[test]
    fn test_daily_usage_exceeds() {
        let today = Utc::now().date_naive();
        let mut usage = DailyUsage::new(today);
        usage.add_bytes(1000);

        assert!(!usage.exceeds(0)); // 0 = unlimited
        assert!(!usage.exceeds(2000));
        assert!(usage.exceeds(1000));
        assert!(usage.exceeds(500));
    }

    #[test]
    fn test_daily_usage_remaining() {
        let today = Utc::now().date_naive();
        let mut usage = DailyUsage::new(today);
        usage.add_bytes(1000);

        assert_eq!(usage.remaining(0), u64::MAX); // unlimited
        assert_eq!(usage.remaining(2000), 1000);
        assert_eq!(usage.remaining(1000), 0);
        assert_eq!(usage.remaining(500), 0); // saturating_sub
    }

    #[test]
    fn test_daily_usage_percent() {
        let today = Utc::now().date_naive();
        let mut usage = DailyUsage::new(today);
        usage.add_bytes(500);

        assert_eq!(usage.usage_percent(0), 0.0); // unlimited = 0%
        assert_eq!(usage.usage_percent(1000), 50.0);
        assert_eq!(usage.usage_percent(500), 100.0);
        assert!(usage.usage_percent(250) > 100.0); // over limit
    }

    #[test]
    fn test_data_cap_manager_new() {
        let manager = DataCapManager::new();
        assert!(!manager.config().enabled);
        assert!(!manager.is_cap_reached());
        assert!(!manager.is_auto_paused());
    }

    #[test]
    fn test_data_cap_manager_set_config() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 1024 * 1024)); // 1MB

        assert!(manager.config().enabled);
        assert_eq!(manager.config().daily_limit_bytes, 1024 * 1024);
    }

    #[test]
    fn test_data_cap_manager_record_download() {
        let mut manager = DataCapManager::new();
        manager.set_daily_limit(1000);
        manager.set_enabled(true);

        let crossed = manager.record_download("task-1", 500);
        assert!(!crossed); // below cap
        assert!(!manager.is_cap_reached());

        let crossed = manager.record_download("task-1", 400);
        assert!(!crossed); // still below

        let crossed = manager.record_download("task-2", 200);
        assert!(crossed); // just crossed the cap!
        assert!(manager.is_cap_reached());
    }

    #[test]
    fn test_data_cap_manager_disabled_no_tracking() {
        let mut manager = DataCapManager::new();
        // disabled by default

        let crossed = manager.record_download("task-1", 999999);
        assert!(!crossed);
        assert!(!manager.is_cap_reached());
    }

    #[test]
    fn test_data_cap_manager_unlimited() {
        let mut manager = DataCapManager::new();
        manager.set_enabled(true);
        manager.set_daily_limit(0); // unlimited

        let crossed = manager.record_download("task-1", 999999999);
        assert!(!crossed);
        assert!(!manager.is_cap_reached());
    }

    #[test]
    fn test_data_cap_manager_task_counting() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 10000));

        // Same task records multiple times but counts once
        manager.record_download("task-1", 100);
        manager.record_download("task-1", 200);
        manager.record_download("task-1", 300);

        assert_eq!(manager.today_usage().task_count, 1);
        assert_eq!(manager.today_usage().bytes_downloaded, 600);

        // Different task counts separately
        manager.record_download("task-2", 100);
        assert_eq!(manager.today_usage().task_count, 2);
    }

    #[test]
    fn test_data_cap_manager_auto_pause() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 100));

        manager.record_download("task-1", 150);
        assert!(manager.is_cap_reached());
        assert!(manager.should_pause_downloads());

        manager.mark_auto_paused();
        assert!(manager.is_auto_paused());
        assert!(!manager.should_pause_downloads()); // already paused

        manager.clear_auto_paused();
        assert!(manager.should_pause_downloads());
    }

    #[test]
    fn test_data_cap_manager_reset_today() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 1000));
        manager.record_download("task-1", 500);
        manager.mark_auto_paused();

        manager.reset_today();
        assert_eq!(manager.today_usage().bytes_downloaded, 0);
        assert_eq!(manager.today_usage().task_count, 0);
        assert!(!manager.is_auto_paused());
        assert!(!manager.is_cap_reached());
    }

    #[test]
    fn test_data_cap_status_display() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 1024 * 1024)); // 1MB
        manager.record_download("task-1", 512 * 1024); // 512KB

        let status = manager.status();
        let display = status.format_display();

        assert!(display.contains("Daily Data Cap Status"));
        assert!(display.contains("Active"));
        assert!(display.contains("1.00 MB"));
        assert!(display.contains("512.00 KB"));
    }

    #[test]
    fn test_data_cap_status_display_disabled() {
        let manager = DataCapManager::new();
        let status = manager.status();
        let display = status.format_display();

        assert!(display.contains("Disabled"));
    }

    #[test]
    fn test_data_cap_status_display_cap_reached() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 100));
        manager.record_download("task-1", 150);

        let status = manager.status();
        let display = status.format_display();

        assert!(display.contains("CAP REACHED"));
        assert!(display.contains("Downloads will be paused"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
    }

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1KB"), Some(1024));
        assert_eq!(parse_size("1kb"), Some(1024));
        assert_eq!(parse_size("1 MB"), Some(1024 * 1024));
        assert_eq!(parse_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("1TB"), Some(1024u64 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("500MB"), Some(500 * 1024 * 1024));
        assert_eq!(
            parse_size("1.5GB"),
            Some((1.5 * 1024.0 * 1024.0 * 1024.0) as u64)
        );
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("abc"), None);
        assert_eq!(parse_size("1XB"), None);
    }

    #[test]
    fn test_save_load_data_cap() {
        let temp_dir = std::env::temp_dir().join("test_data_cap_save_load");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 1024 * 1024));
        manager.record_download("task-1", 512 * 1024);

        save_data_cap(&manager, &temp_dir).unwrap();

        let loaded = load_data_cap(&temp_dir).unwrap();
        assert!(loaded.config().enabled);
        assert_eq!(loaded.config().daily_limit_bytes, 1024 * 1024);
        assert_eq!(loaded.today_usage().bytes_downloaded, 512 * 1024);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_data_cap_nonexistent() {
        let temp_dir = std::env::temp_dir().join("test_data_cap_nonexistent");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let loaded = load_data_cap(&temp_dir);
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_data_cap_persistence_auto_paused() {
        let temp_dir = std::env::temp_dir().join("test_data_cap_auto_paused");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 100));
        manager.record_download("task-1", 150);
        manager.mark_auto_paused();

        save_data_cap(&manager, &temp_dir).unwrap();

        let loaded = load_data_cap(&temp_dir).unwrap();
        assert!(loaded.is_auto_paused());
        assert!(loaded.is_cap_reached());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_data_cap_remaining_bytes() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 1000));

        assert_eq!(manager.remaining_bytes(), 1000);
        manager.record_download("task-1", 300);
        assert_eq!(manager.remaining_bytes(), 700);
        manager.record_download("task-2", 800);
        assert_eq!(manager.remaining_bytes(), 0); // saturating
    }

    #[test]
    fn test_data_cap_day_rollover() {
        let mut manager = DataCapManager::new();
        manager.set_config(DataCapConfig::new(true, 1000));
        manager.record_download("task-1", 500);

        // Simulate yesterday's date
        let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
        manager.current_usage.date = yesterday;

        // Recording should trigger rollover
        manager.record_download("task-2", 100);

        // Should have reset
        assert_eq!(manager.today_usage().date, Utc::now().date_naive());
        assert_eq!(manager.today_usage().bytes_downloaded, 100);
        assert_eq!(manager.today_usage().task_count, 1);
    }
}
