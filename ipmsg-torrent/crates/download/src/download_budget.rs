//! Weekly/Monthly Download Budget System
//!
//! Extends the daily data cap with weekly and monthly budget tracking.
//! When any budget limit is reached, downloads are automatically paused.
//! Supports budget rollover, remaining allocation queries, and per-budget
//! usage history.

use chrono::{DateTime, Datelike, NaiveDate, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Configuration for the download budget system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetConfig {
    /// Whether the budget system is enabled
    pub enabled: bool,
    /// Weekly download budget in bytes (0 = unlimited)
    pub weekly_limit_bytes: u64,
    /// Monthly download budget in bytes (0 = unlimited)
    pub monthly_limit_bytes: u64,
    /// Whether to auto-pause downloads when a budget is exhausted
    pub auto_pause: bool,
    /// Day of week when the weekly budget resets (default: Monday)
    pub week_start_day: Weekday,
    /// Day of month when the monthly budget resets (default: 1st)
    /// Clamped to valid day for months with fewer days.
    pub month_start_day: u32,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            weekly_limit_bytes: 0,
            monthly_limit_bytes: 0,
            auto_pause: true,
            week_start_day: Weekday::Mon,
            month_start_day: 1,
        }
    }
}

impl BudgetConfig {
    pub fn new(weekly_limit_bytes: u64, monthly_limit_bytes: u64) -> Self {
        Self {
            enabled: true,
            weekly_limit_bytes,
            monthly_limit_bytes,
            ..Default::default()
        }
    }
}

/// Tracks usage for a single period (week or month)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodUsage {
    /// Start date of this period (inclusive)
    pub period_start: NaiveDate,
    /// End date of this period (inclusive, None = ongoing)
    pub period_end: Option<NaiveDate>,
    /// Total bytes downloaded in this period
    pub bytes_downloaded: u64,
    /// Number of distinct tasks that contributed
    pub task_count: u32,
    /// Last update timestamp
    pub last_updated: DateTime<Utc>,
}

impl PeriodUsage {
    pub fn new(period_start: NaiveDate) -> Self {
        Self {
            period_start,
            period_end: None,
            bytes_downloaded: 0,
            task_count: 0,
            last_updated: Utc::now(),
        }
    }

    /// Record bytes downloaded
    pub fn add_bytes(&mut self, bytes: u64) {
        self.bytes_downloaded += bytes;
        self.last_updated = Utc::now();
    }

    /// Increment task count
    pub fn increment_task_count(&mut self) {
        self.task_count += 1;
        self.last_updated = Utc::now();
    }

    /// Check if usage exceeds a limit
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

/// Summary of budget status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetSummary {
    /// Current configuration
    pub config: BudgetConfig,
    /// Current week usage
    pub weekly_usage: PeriodUsage,
    /// Current month usage
    pub monthly_usage: PeriodUsage,
    /// Whether the weekly budget is exhausted
    pub weekly_exhausted: bool,
    /// Whether the monthly budget is exhausted
    pub monthly_exhausted: bool,
    /// Whether any budget is exhausted (downloads should pause)
    pub any_exhausted: bool,
    /// Bytes remaining this week (u64::MAX if unlimited)
    pub weekly_remaining: u64,
    /// Bytes remaining this month (u64::MAX if unlimited)
    pub monthly_remaining: u64,
    /// Whether downloads were auto-paused
    pub auto_paused: bool,
}

impl BudgetSummary {
    pub fn format_display(&self) -> String {
        let mut out = String::from("📅 Download Budget Status\n");

        if !self.config.enabled {
            out.push_str("  Status: Disabled\n");
            return out;
        }

        // Weekly
        out.push_str(&format!(
            "  Weekly:  {} / {} ({:.1}%)\n",
            format_bytes(self.weekly_usage.bytes_downloaded),
            if self.config.weekly_limit_bytes == 0 {
                "∞".to_string()
            } else {
                format_bytes(self.config.weekly_limit_bytes)
            },
            self.weekly_usage
                .usage_percent(self.config.weekly_limit_bytes)
        ));
        let wr = if self.weekly_remaining == u64::MAX {
            "∞".to_string()
        } else {
            format_bytes(self.weekly_remaining)
        };
        out.push_str(&format!(
            "    Remaining: {} | Tasks: {}\n",
            wr, self.weekly_usage.task_count
        ));
        if self.weekly_exhausted {
            out.push_str("    🔴 Weekly budget exhausted\n");
        }

        // Monthly
        out.push_str(&format!(
            "  Monthly: {} / {} ({:.1}%)\n",
            format_bytes(self.monthly_usage.bytes_downloaded),
            if self.config.monthly_limit_bytes == 0 {
                "∞".to_string()
            } else {
                format_bytes(self.config.monthly_limit_bytes)
            },
            self.monthly_usage
                .usage_percent(self.config.monthly_limit_bytes)
        ));
        let mr = if self.monthly_remaining == u64::MAX {
            "∞".to_string()
        } else {
            format_bytes(self.monthly_remaining)
        };
        out.push_str(&format!(
            "    Remaining: {} | Tasks: {}\n",
            mr, self.monthly_usage.task_count
        ));
        if self.monthly_exhausted {
            out.push_str("    🔴 Monthly budget exhausted\n");
        }

        if self.any_exhausted {
            out.push_str("  ⚠️  Downloads will be paused until budget resets\n");
        }

        out
    }
}

/// Manager for weekly/monthly download budget tracking
#[derive(Debug, Clone)]
pub struct BudgetManager {
    config: BudgetConfig,
    weekly_usage: PeriodUsage,
    monthly_usage: PeriodUsage,
    /// Tasks counted this week
    weekly_tasks: std::collections::HashSet<String>,
    /// Tasks counted this month
    monthly_tasks: std::collections::HashSet<String>,
    /// Whether downloads were auto-paused
    auto_paused: bool,
}

impl Default for BudgetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BudgetManager {
    pub fn new() -> Self {
        let today = Utc::now().date_naive();
        Self {
            config: BudgetConfig::default(),
            weekly_usage: PeriodUsage::new(today),
            monthly_usage: PeriodUsage::new(today),
            weekly_tasks: std::collections::HashSet::new(),
            monthly_tasks: std::collections::HashSet::new(),
            auto_paused: false,
        }
    }

    /// Set configuration
    pub fn set_config(&mut self, config: BudgetConfig) {
        self.config = config;
    }

    /// Get current configuration
    pub fn config(&self) -> &BudgetConfig {
        &self.config
    }

    /// Enable or disable
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Set weekly limit in bytes
    pub fn set_weekly_limit(&mut self, bytes: u64) {
        self.config.weekly_limit_bytes = bytes;
    }

    /// Set monthly limit in bytes
    pub fn set_monthly_limit(&mut self, bytes: u64) {
        self.config.monthly_limit_bytes = bytes;
    }

    /// Get full budget summary
    pub fn summary(&self) -> BudgetSummary {
        let weekly_exhausted = self.is_weekly_exhausted();
        let monthly_exhausted = self.is_monthly_exhausted();
        BudgetSummary {
            config: self.config.clone(),
            weekly_usage: self.weekly_usage.clone(),
            monthly_usage: self.monthly_usage.clone(),
            weekly_exhausted,
            monthly_exhausted,
            any_exhausted: weekly_exhausted || monthly_exhausted,
            weekly_remaining: self.weekly_remaining(),
            monthly_remaining: self.monthly_remaining(),
            auto_paused: self.auto_paused,
        }
    }

    /// Check if weekly budget is exhausted
    pub fn is_weekly_exhausted(&self) -> bool {
        if !self.config.enabled || self.config.weekly_limit_bytes == 0 {
            return false;
        }
        self.weekly_usage.exceeds(self.config.weekly_limit_bytes)
    }

    /// Check if monthly budget is exhausted
    pub fn is_monthly_exhausted(&self) -> bool {
        if !self.config.enabled || self.config.monthly_limit_bytes == 0 {
            return false;
        }
        self.monthly_usage.exceeds(self.config.monthly_limit_bytes)
    }

    /// Check if downloads should be paused
    pub fn should_pause_downloads(&self) -> bool {
        self.config.auto_pause
            && !self.auto_paused
            && (self.is_weekly_exhausted() || self.is_monthly_exhausted())
    }

    /// Mark downloads as auto-paused
    pub fn mark_auto_paused(&mut self) {
        self.auto_paused = true;
    }

    /// Clear auto-paused flag
    pub fn clear_auto_paused(&mut self) {
        self.auto_paused = false;
    }

    /// Whether downloads were auto-paused
    pub fn is_auto_paused(&self) -> bool {
        self.auto_paused
    }

    /// Record bytes downloaded for a task.
    /// Returns true if this caused any budget to be newly exhausted.
    pub fn record_download(&mut self, task_id: &str, bytes: u64) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Check for period rollovers
        self.check_rollovers();

        let was_below = !self.is_weekly_exhausted() && !self.is_monthly_exhausted();

        self.weekly_usage.add_bytes(bytes);
        self.monthly_usage.add_bytes(bytes);

        // Count task once per period
        if !self.weekly_tasks.contains(task_id) {
            self.weekly_tasks.insert(task_id.to_string());
            self.weekly_usage.increment_task_count();
        }
        if !self.monthly_tasks.contains(task_id) {
            self.monthly_tasks.insert(task_id.to_string());
            self.monthly_usage.increment_task_count();
        }

        let now_above = self.is_weekly_exhausted() || self.is_monthly_exhausted();

        was_below && now_above
    }

    /// Check and perform period rollovers if needed
    pub fn check_rollovers(&mut self) {
        let today = Utc::now().date_naive();

        // Weekly rollover: if today is on or after the next week_start_day
        let next_week_start =
            next_weekday_on_or_after(self.weekly_usage.period_start, self.config.week_start_day)
                + chrono::Duration::days(7);
        if today >= next_week_start {
            self.weekly_usage = PeriodUsage::new(next_week_start);
            self.weekly_tasks.clear();
            self.auto_paused = false;
        }

        // Monthly rollover: if today is on or after the next month_start_day
        let next_month_start = next_month_start_on_or_after(
            self.monthly_usage.period_start,
            self.config.month_start_day,
        );
        if today >= next_month_start {
            self.monthly_usage = PeriodUsage::new(next_month_start);
            self.monthly_tasks.clear();
            self.auto_paused = false;
        }
    }

    /// Get bytes remaining this week
    pub fn weekly_remaining(&self) -> u64 {
        self.weekly_usage.remaining(self.config.weekly_limit_bytes)
    }

    /// Get bytes remaining this month
    pub fn monthly_remaining(&self) -> u64 {
        self.monthly_usage
            .remaining(self.config.monthly_limit_bytes)
    }

    /// Get current weekly usage
    pub fn weekly_usage(&self) -> &PeriodUsage {
        &self.weekly_usage
    }

    /// Get current monthly usage
    pub fn monthly_usage(&self) -> &PeriodUsage {
        &self.monthly_usage
    }

    /// Reset both periods (for testing or manual reset)
    pub fn reset(&mut self) {
        let today = Utc::now().date_naive();
        self.weekly_usage = PeriodUsage::new(today);
        self.monthly_usage = PeriodUsage::new(today);
        self.weekly_tasks.clear();
        self.monthly_tasks.clear();
        self.auto_paused = false;
    }
}

// --- Date helpers ---

/// Find the most recent occurrence of `target_day` on or before `date`.
fn weekday_on_or_before(date: NaiveDate, target_day: Weekday) -> NaiveDate {
    let diff = (date.weekday().num_days_from_monday() as i32
        - target_day.num_days_from_monday() as i32
        + 7)
        % 7;
    date - chrono::Duration::days(diff as i64)
}

/// Find the next occurrence of `target_day` strictly after `date`.
fn next_weekday_on_or_after(date: NaiveDate, target_day: Weekday) -> NaiveDate {
    weekday_on_or_before(date, target_day)
}

/// Find the next month start (based on `start_day`) strictly after `period_start`.
fn next_month_start_on_or_after(period_start: NaiveDate, start_day: u32) -> NaiveDate {
    // The current period started on or near `start_day` of some month.
    // The next period starts on `start_day` of the following month.
    let year = period_start.year();
    let month = period_start.month();
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    // Clamp start_day to valid day for the target month
    let max_day = days_in_month(next_year, next_month);
    let day = start_day.min(max_day);
    NaiveDate::from_ymd_opt(next_year, next_month, day).unwrap_or(period_start)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    // Use the trick: go to the first of next month, subtract one day
    let (y, m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(y, m, 1).unwrap();
    let last = first_next - chrono::Duration::days(1);
    last.day()
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
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    if let Ok(bytes) = s.parse::<u64>() {
        return Some(bytes);
    }

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
        "T" | "TB" => 1024u64 * 1024 * 1024 * 1024,
        _ => return None,
    };

    Some((num * multiplier as f64) as u64)
}

// --- Persistence ---

/// Data structure for persistence
#[derive(Debug, Serialize, Deserialize)]
struct BudgetPersistData {
    config: BudgetConfig,
    weekly_usage: PeriodUsage,
    monthly_usage: PeriodUsage,
    auto_paused: bool,
}

/// Save budget config and usage to disk
pub fn save_budget(manager: &BudgetManager, data_dir: &Path) -> Result<(), std::io::Error> {
    let path = data_dir.join("download_budget.json");

    let data = BudgetPersistData {
        config: manager.config().clone(),
        weekly_usage: manager.weekly_usage().clone(),
        monthly_usage: manager.monthly_usage().clone(),
        auto_paused: manager.is_auto_paused(),
    };

    let json = serde_json::to_string_pretty(&data).map_err(std::io::Error::other)?;

    let temp_path = path.with_extension("json.tmp");
    std::fs::write(&temp_path, json)?;
    std::fs::rename(&temp_path, &path)?;
    Ok(())
}

/// Load budget config and usage from disk
pub fn load_budget(data_dir: &Path) -> Option<BudgetManager> {
    let path = data_dir.join("download_budget.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let data: BudgetPersistData = serde_json::from_str(&json).ok()?;

    let mut manager = BudgetManager::new();
    manager.config = data.config;
    manager.weekly_usage = data.weekly_usage;
    manager.monthly_usage = data.monthly_usage;
    manager.auto_paused = data.auto_paused;

    manager.check_rollovers();

    Some(manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_budget_config_default() {
        let config = BudgetConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.weekly_limit_bytes, 0);
        assert_eq!(config.monthly_limit_bytes, 0);
        assert!(config.auto_pause);
        assert_eq!(config.week_start_day, Weekday::Mon);
        assert_eq!(config.month_start_day, 1);
    }

    #[test]
    fn test_budget_config_new() {
        let config = BudgetConfig::new(1_000_000, 5_000_000);
        assert!(config.enabled);
        assert_eq!(config.weekly_limit_bytes, 1_000_000);
        assert_eq!(config.monthly_limit_bytes, 5_000_000);
    }

    #[test]
    fn test_period_usage_new() {
        let today = Utc::now().date_naive();
        let usage = PeriodUsage::new(today);
        assert_eq!(usage.period_start, today);
        assert!(usage.period_end.is_none());
        assert_eq!(usage.bytes_downloaded, 0);
        assert_eq!(usage.task_count, 0);
    }

    #[test]
    fn test_period_usage_add_bytes() {
        let today = Utc::now().date_naive();
        let mut usage = PeriodUsage::new(today);
        usage.add_bytes(1024);
        usage.add_bytes(2048);
        assert_eq!(usage.bytes_downloaded, 3072);
    }

    #[test]
    fn test_period_usage_exceeds() {
        let today = Utc::now().date_naive();
        let mut usage = PeriodUsage::new(today);
        usage.add_bytes(1000);

        assert!(!usage.exceeds(0));
        assert!(!usage.exceeds(2000));
        assert!(usage.exceeds(1000));
        assert!(usage.exceeds(500));
    }

    #[test]
    fn test_period_usage_remaining() {
        let today = Utc::now().date_naive();
        let mut usage = PeriodUsage::new(today);
        usage.add_bytes(1000);

        assert_eq!(usage.remaining(0), u64::MAX);
        assert_eq!(usage.remaining(2000), 1000);
        assert_eq!(usage.remaining(1000), 0);
        assert_eq!(usage.remaining(500), 0);
    }

    #[test]
    fn test_period_usage_percent() {
        let today = Utc::now().date_naive();
        let mut usage = PeriodUsage::new(today);
        usage.add_bytes(500);

        assert_eq!(usage.usage_percent(0), 0.0);
        assert_eq!(usage.usage_percent(1000), 50.0);
        assert_eq!(usage.usage_percent(500), 100.0);
        assert!(usage.usage_percent(250) > 100.0);
    }

    #[test]
    fn test_budget_manager_new() {
        let manager = BudgetManager::new();
        assert!(!manager.config().enabled);
        assert!(!manager.is_weekly_exhausted());
        assert!(!manager.is_monthly_exhausted());
        assert!(!manager.is_auto_paused());
    }

    #[test]
    fn test_budget_manager_set_config() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1_000_000, 5_000_000));

        assert!(manager.config().enabled);
        assert_eq!(manager.config().weekly_limit_bytes, 1_000_000);
        assert_eq!(manager.config().monthly_limit_bytes, 5_000_000);
    }

    #[test]
    fn test_budget_manager_record_download() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1000, 5000));

        let crossed = manager.record_download("task-1", 500);
        assert!(!crossed);
        assert!(!manager.is_weekly_exhausted());

        let crossed = manager.record_download("task-1", 400);
        assert!(!crossed);

        let crossed = manager.record_download("task-2", 200);
        assert!(crossed); // weekly just crossed 1000
        assert!(manager.is_weekly_exhausted());
        assert!(!manager.is_monthly_exhausted()); // monthly still fine
    }

    #[test]
    fn test_budget_manager_monthly_exhausted() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(10_000, 1000));

        let crossed = manager.record_download("task-1", 1100);
        assert!(crossed); // monthly crossed
        assert!(!manager.is_weekly_exhausted()); // weekly still fine
        assert!(manager.is_monthly_exhausted());
    }

    #[test]
    fn test_budget_manager_disabled() {
        let mut manager = BudgetManager::new();
        // disabled by default

        let crossed = manager.record_download("task-1", 999_999);
        assert!(!crossed);
        assert!(!manager.is_weekly_exhausted());
        assert!(!manager.is_monthly_exhausted());
    }

    #[test]
    fn test_budget_manager_unlimited() {
        let mut manager = BudgetManager::new();
        let mut config = BudgetConfig::default();
        config.enabled = true;
        config.weekly_limit_bytes = 0; // unlimited
        config.monthly_limit_bytes = 0; // unlimited
        manager.set_config(config);

        let crossed = manager.record_download("task-1", 999_999_999);
        assert!(!crossed);
        assert!(!manager.is_weekly_exhausted());
        assert!(!manager.is_monthly_exhausted());
    }

    #[test]
    fn test_budget_manager_task_counting() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(10_000, 50_000));

        manager.record_download("task-1", 100);
        manager.record_download("task-1", 200);
        manager.record_download("task-1", 300);

        assert_eq!(manager.weekly_usage().task_count, 1);
        assert_eq!(manager.weekly_usage().bytes_downloaded, 600);
        assert_eq!(manager.monthly_usage().task_count, 1);
        assert_eq!(manager.monthly_usage().bytes_downloaded, 600);

        manager.record_download("task-2", 100);
        assert_eq!(manager.weekly_usage().task_count, 2);
        assert_eq!(manager.monthly_usage().task_count, 2);
    }

    #[test]
    fn test_budget_manager_auto_pause() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(100, 10_000));

        manager.record_download("task-1", 150);
        assert!(manager.is_weekly_exhausted());
        assert!(manager.should_pause_downloads());

        manager.mark_auto_paused();
        assert!(manager.is_auto_paused());
        assert!(!manager.should_pause_downloads()); // already paused

        manager.clear_auto_paused();
        assert!(manager.should_pause_downloads());
    }

    #[test]
    fn test_budget_manager_reset() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1000, 5000));
        manager.record_download("task-1", 500);
        manager.mark_auto_paused();

        manager.reset();
        assert_eq!(manager.weekly_usage().bytes_downloaded, 0);
        assert_eq!(manager.monthly_usage().bytes_downloaded, 0);
        assert_eq!(manager.weekly_usage().task_count, 0);
        assert_eq!(manager.monthly_usage().task_count, 0);
        assert!(!manager.is_auto_paused());
        assert!(!manager.is_weekly_exhausted());
    }

    #[test]
    fn test_budget_manager_remaining() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1000, 5000));

        assert_eq!(manager.weekly_remaining(), 1000);
        assert_eq!(manager.monthly_remaining(), 5000);

        manager.record_download("task-1", 300);
        assert_eq!(manager.weekly_remaining(), 700);
        assert_eq!(manager.monthly_remaining(), 4700);
    }

    #[test]
    fn test_budget_summary_display() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1024 * 1024, 10 * 1024 * 1024));
        manager.record_download("task-1", 512 * 1024);

        let summary = manager.summary();
        let display = summary.format_display();

        assert!(display.contains("Download Budget Status"));
        assert!(display.contains("Weekly:"));
        assert!(display.contains("Monthly:"));
        assert!(display.contains("512.00 KB"));
    }

    #[test]
    fn test_budget_summary_display_disabled() {
        let manager = BudgetManager::new();
        let summary = manager.summary();
        let display = summary.format_display();

        assert!(display.contains("Disabled"));
    }

    #[test]
    fn test_budget_summary_display_exhausted() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(100, 10_000));
        manager.record_download("task-1", 150);

        let summary = manager.summary();
        let display = summary.format_display();

        assert!(display.contains("Weekly budget exhausted"));
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
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("abc"), None);
        assert_eq!(parse_size("1XB"), None);
    }

    #[test]
    fn test_save_load_budget() {
        let temp_dir = std::env::temp_dir().join("test_download_budget_save_load");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1_000_000, 5_000_000));
        manager.record_download("task-1", 500_000);

        save_budget(&manager, &temp_dir).unwrap();

        let loaded = load_budget(&temp_dir).unwrap();
        assert!(loaded.config().enabled);
        assert_eq!(loaded.config().weekly_limit_bytes, 1_000_000);
        assert_eq!(loaded.config().monthly_limit_bytes, 5_000_000);
        assert_eq!(loaded.weekly_usage().bytes_downloaded, 500_000);
        assert_eq!(loaded.monthly_usage().bytes_downloaded, 500_000);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_budget_nonexistent() {
        let temp_dir = std::env::temp_dir().join("test_download_budget_nonexistent");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let loaded = load_budget(&temp_dir);
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_save_load_budget_auto_paused() {
        let temp_dir = std::env::temp_dir().join("test_download_budget_auto_paused");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(100, 10_000));
        manager.record_download("task-1", 150);
        manager.mark_auto_paused();

        save_budget(&manager, &temp_dir).unwrap();

        let loaded = load_budget(&temp_dir).unwrap();
        assert!(loaded.is_auto_paused());
        assert!(loaded.is_weekly_exhausted());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_days_in_month() {
        assert_eq!(days_in_month(2024, 2), 29); // leap year
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(2024, 1), 31);
        assert_eq!(days_in_month(2024, 4), 30);
        assert_eq!(days_in_month(2024, 12), 31);
    }

    #[test]
    fn test_weekday_on_or_before() {
        // 2024-01-10 is Wednesday
        let wed = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        assert_eq!(wed.weekday(), Weekday::Wed);

        // Monday on or before Wed Jan 10 => Mon Jan 8
        let mon = weekday_on_or_before(wed, Weekday::Mon);
        assert_eq!(mon.weekday(), Weekday::Mon);
        assert_eq!(mon, NaiveDate::from_ymd_opt(2024, 1, 8).unwrap());

        // Wednesday on or before Wed Jan 10 => Wed Jan 10 (same day)
        let same = weekday_on_or_before(wed, Weekday::Wed);
        assert_eq!(same, wed);
    }

    #[test]
    fn test_next_month_start_on_or_after() {
        // Period started Jan 8 (Monday), month_start_day = 1
        // Next period should start Feb 1
        let start = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        let next = next_month_start_on_or_after(start, 1);
        assert_eq!(next, NaiveDate::from_ymd_opt(2024, 2, 1).unwrap());

        // Period started Jan 15, month_start_day = 15
        // Next period should start Feb 15
        let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let next = next_month_start_on_or_after(start, 15);
        assert_eq!(next, NaiveDate::from_ymd_opt(2024, 2, 15).unwrap());

        // Period started Jan 30, month_start_day = 31
        // Feb doesn't have 31 days, so clamp to Feb 29 (2024 is leap)
        let start = NaiveDate::from_ymd_opt(2024, 1, 30).unwrap();
        let next = next_month_start_on_or_after(start, 31);
        assert_eq!(next, NaiveDate::from_ymd_opt(2024, 2, 29).unwrap());
    }

    #[test]
    fn test_budget_weekly_rollover_simulation() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(1000, 10_000));

        manager.record_download("task-1", 500);
        assert_eq!(manager.weekly_usage().bytes_downloaded, 500);

        // Simulate: set weekly period start to 8 days ago
        let eight_days_ago = Utc::now().date_naive() - chrono::Duration::days(8);
        manager.weekly_usage.period_start = eight_days_ago;

        // Next record should trigger weekly rollover
        manager.record_download("task-2", 100);

        // Weekly should have reset
        assert_eq!(manager.weekly_usage().bytes_downloaded, 100);
        assert_eq!(manager.weekly_usage().task_count, 1);
        // Monthly should still have accumulated
        assert_eq!(manager.monthly_usage().bytes_downloaded, 600);
    }

    #[test]
    fn test_budget_monthly_rollover_simulation() {
        let mut manager = BudgetManager::new();
        manager.set_config(BudgetConfig::new(10_000, 1000));

        manager.record_download("task-1", 500);
        assert_eq!(manager.monthly_usage().bytes_downloaded, 500);

        // Simulate: set monthly period start to 2 months ago
        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(62);
        manager.monthly_usage.period_start = two_months_ago;

        // Next record should trigger monthly rollover
        manager.record_download("task-2", 100);

        // Monthly should have reset
        assert_eq!(manager.monthly_usage().bytes_downloaded, 100);
        assert_eq!(manager.monthly_usage().task_count, 1);
        // Weekly should still have accumulated (unless it also rolled)
        assert!(manager.weekly_usage().bytes_downloaded >= 600);
    }

    #[test]
    fn test_budget_manager_should_pause_requires_auto_pause() {
        let mut manager = BudgetManager::new();
        let mut config = BudgetConfig::new(100, 10_000);
        config.auto_pause = false;
        manager.set_config(config);

        manager.record_download("task-1", 150);
        assert!(manager.is_weekly_exhausted());
        // auto_pause is disabled, so should_pause_downloads returns false
        assert!(!manager.should_pause_downloads());
    }

    #[test]
    fn test_budget_set_individual_limits() {
        let mut manager = BudgetManager::new();
        manager.set_enabled(true);
        manager.set_weekly_limit(5_000_000);
        manager.set_monthly_limit(20_000_000);

        assert_eq!(manager.config().weekly_limit_bytes, 5_000_000);
        assert_eq!(manager.config().monthly_limit_bytes, 20_000_000);
    }
}
