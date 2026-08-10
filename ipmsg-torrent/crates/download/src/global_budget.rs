//! Global Download Budget System
//!
//! Provides weekly and monthly download data budgets with automatic pause
//! when limits are reached. Complements the existing daily data cap system
//! with longer-term budget tracking.
//!
//! Features:
//! - Weekly budget: resets at the start of each week (Monday 00:00 UTC)
//! - Monthly budget: resets at the start of each month (1st 00:00 UTC)
//! - Automatic pause when budget is exhausted
//! - Configurable warning thresholds (e.g., notify at 80% usage)
//! - Budget usage tracking and summary
//! - Persistent configuration and usage data

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Configuration for the global download budget system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalBudgetConfig {
    /// Whether the global budget system is enabled
    pub enabled: bool,
    /// Weekly budget limit in bytes (0 = unlimited)
    pub weekly_limit_bytes: u64,
    /// Monthly budget limit in bytes (0 = unlimited)
    pub monthly_limit_bytes: u64,
    /// Warning threshold percentage (0.0-1.0, e.g., 0.8 = warn at 80%)
    pub warning_threshold: f64,
    /// Whether to automatically pause downloads when budget is exceeded
    pub auto_pause_on_exceed: bool,
}

impl Default for GlobalBudgetConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            weekly_limit_bytes: 0,
            monthly_limit_bytes: 0,
            warning_threshold: 0.8,
            auto_pause_on_exceed: true,
        }
    }
}

impl GlobalBudgetConfig {
    /// Create a new configuration with the given limits
    pub fn new(weekly_limit_bytes: u64, monthly_limit_bytes: u64) -> Self {
        Self {
            enabled: true,
            weekly_limit_bytes,
            monthly_limit_bytes,
            warning_threshold: 0.8,
            auto_pause_on_exceed: true,
        }
    }

    /// Set the warning threshold
    pub fn with_warning_threshold(mut self, threshold: f64) -> Self {
        self.warning_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set whether to auto-pause on exceed
    pub fn with_auto_pause(mut self, auto_pause: bool) -> Self {
        self.auto_pause_on_exceed = auto_pause;
        self
    }
}

/// Tracks usage for a specific period (week or month)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodUsage {
    /// The start date of this period
    pub period_start: NaiveDate,
    /// The end date of this period (exclusive)
    pub period_end: NaiveDate,
    /// Total bytes downloaded in this period
    pub bytes_downloaded: u64,
    /// Number of tasks that contributed to this usage
    pub task_count: u32,
    /// Last time this record was updated
    pub last_updated: DateTime<Utc>,
}

impl PeriodUsage {
    /// Create a new period usage record
    pub fn new(period_start: NaiveDate, period_end: NaiveDate) -> Self {
        Self {
            period_start,
            period_end,
            bytes_downloaded: 0,
            task_count: 0,
            last_updated: Utc::now(),
        }
    }

    /// Record downloaded bytes
    pub fn add_bytes(&mut self, bytes: u64) {
        self.bytes_downloaded += bytes;
        self.last_updated = Utc::now();
    }

    /// Increment the task count
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

    /// Get usage as a percentage of the limit (0.0-1.0+)
    pub fn usage_percent(&self, limit_bytes: u64) -> f64 {
        if limit_bytes == 0 {
            return 0.0;
        }
        self.bytes_downloaded as f64 / limit_bytes as f64
    }

    /// Check if the period is still active (current date is within period)
    pub fn is_active(&self, today: NaiveDate) -> bool {
        today >= self.period_start && today < self.period_end
    }

    /// Check if the period has expired
    pub fn is_expired(&self, today: NaiveDate) -> bool {
        today >= self.period_end
    }
}

/// Status of the global budget
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetStatus {
    /// Budget is not configured or disabled
    Inactive,
    /// Budget is active and within limits
    Active,
    /// Budget usage has exceeded the warning threshold
    Warning,
    /// Budget has been fully exhausted
    Exceeded,
}

impl BudgetStatus {
    /// Get a human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            BudgetStatus::Inactive => "Inactive",
            BudgetStatus::Active => "Active",
            BudgetStatus::Warning => "Warning",
            BudgetStatus::Exceeded => "Exceeded",
        }
    }

    /// Get an emoji indicator
    pub fn emoji(&self) -> &'static str {
        match self {
            BudgetStatus::Inactive => "⚪",
            BudgetStatus::Active => "🟢",
            BudgetStatus::Warning => "🟡",
            BudgetStatus::Exceeded => "🔴",
        }
    }
}

impl std::fmt::Display for BudgetStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.emoji(), self.label())
    }
}

/// Summary of global budget usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalBudgetSummary {
    /// Current budget status (worst of weekly/monthly)
    pub status: BudgetStatus,
    /// Weekly usage information
    pub weekly: PeriodSummary,
    /// Monthly usage information
    pub monthly: PeriodSummary,
    /// Whether downloads are currently paused due to budget
    pub downloads_paused: bool,
}

impl GlobalBudgetSummary {
    /// Format the summary as a human-readable string
    pub fn format_summary(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("Global Budget Status: {}\n", self.status));
        out.push('\n');

        // Weekly
        out.push_str(&format!(
            "📅 Weekly:  {} / {} ({:.1}%)\n",
            format_bytes(self.weekly.bytes_downloaded),
            if self.weekly.limit_bytes == 0 {
                "∞".to_string()
            } else {
                format_bytes(self.weekly.limit_bytes)
            },
            self.weekly.usage_percent() * 100.0
        ));
        out.push_str(&format!(
            "   Remaining: {}\n",
            if self.weekly.limit_bytes == 0 {
                "∞".to_string()
            } else {
                format_bytes(self.weekly.remaining())
            }
        ));
        out.push_str(&format!(
            "   Period: {} → {}\n",
            self.weekly.period_start, self.weekly.period_end
        ));

        out.push('\n');

        // Monthly
        out.push_str(&format!(
            "📆 Monthly: {} / {} ({:.1}%)\n",
            format_bytes(self.monthly.bytes_downloaded),
            if self.monthly.limit_bytes == 0 {
                "∞".to_string()
            } else {
                format_bytes(self.monthly.limit_bytes)
            },
            self.monthly.usage_percent() * 100.0
        ));
        out.push_str(&format!(
            "   Remaining: {}\n",
            if self.monthly.limit_bytes == 0 {
                "∞".to_string()
            } else {
                format_bytes(self.monthly.remaining())
            }
        ));
        out.push_str(&format!(
            "   Period: {} → {}\n",
            self.monthly.period_start, self.monthly.period_end
        ));

        if self.downloads_paused {
            out.push_str("\n⚠️  Downloads are PAUSED due to budget limit\n");
        }

        out
    }
}

/// Summary for a single period (weekly or monthly)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodSummary {
    /// Period start date
    pub period_start: NaiveDate,
    /// Period end date
    pub period_end: NaiveDate,
    /// Bytes downloaded in this period
    pub bytes_downloaded: u64,
    /// Budget limit for this period
    pub limit_bytes: u64,
    /// Number of tasks
    pub task_count: u32,
    /// Usage percentage (0.0-1.0+)
    pub usage_percent: f64,
    /// Remaining bytes
    pub remaining: u64,
    /// Status for this period
    pub status: BudgetStatus,
}

impl PeriodSummary {
    /// Get the usage as a percentage (0.0-1.0+)
    pub fn usage_percent(&self) -> f64 {
        self.usage_percent
    }

    /// Get the remaining bytes
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
}

/// Manages global download budgets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalBudgetManager {
    /// Configuration
    pub config: GlobalBudgetConfig,
    /// Current weekly usage
    pub weekly_usage: PeriodUsage,
    /// Current monthly usage
    pub monthly_usage: PeriodUsage,
    /// Whether downloads are currently paused due to budget
    pub downloads_paused: bool,
}

impl GlobalBudgetManager {
    /// Create a new budget manager with default config
    pub fn new() -> Self {
        let today = Utc::now().date_naive();
        let (week_start, week_end) = week_bounds(today);
        let (month_start, month_end) = month_bounds(today);

        Self {
            config: GlobalBudgetConfig::default(),
            weekly_usage: PeriodUsage::new(week_start, week_end),
            monthly_usage: PeriodUsage::new(month_start, month_end),
            downloads_paused: false,
        }
    }

    /// Create a new budget manager with the given config
    pub fn with_config(config: GlobalBudgetConfig) -> Self {
        let today = Utc::now().date_naive();
        let (week_start, week_end) = week_bounds(today);
        let (month_start, month_end) = month_bounds(today);

        Self {
            config,
            weekly_usage: PeriodUsage::new(week_start, week_end),
            monthly_usage: PeriodUsage::new(month_start, month_end),
            downloads_paused: false,
        }
    }

    /// Update the configuration
    pub fn set_config(&mut self, config: GlobalBudgetConfig) {
        self.config = config;
    }

    /// Record downloaded bytes and check budget limits
    /// Returns true if the budget is exceeded and downloads should be paused
    pub fn record_download(&mut self, bytes: u64) -> bool {
        if !self.config.enabled {
            return false;
        }

        let today = Utc::now().date_naive();

        // Refresh periods if needed
        self.refresh_periods(today);

        // Record usage
        self.weekly_usage.add_bytes(bytes);
        self.monthly_usage.add_bytes(bytes);

        // Check if budget is exceeded
        self.check_budget_exceeded()
    }

    /// Record a task contributing to the budget
    pub fn record_task(&mut self) {
        let today = Utc::now().date_naive();
        self.refresh_periods(today);
        self.weekly_usage.increment_task_count();
        self.monthly_usage.increment_task_count();
    }

    /// Refresh period boundaries, resetting if the period has expired
    pub fn refresh_periods(&mut self, today: NaiveDate) {
        // Check weekly period
        if self.weekly_usage.is_expired(today) {
            let (week_start, week_end) = week_bounds(today);
            self.weekly_usage = PeriodUsage::new(week_start, week_end);
        }

        // Check monthly period
        if self.monthly_usage.is_expired(today) {
            let (month_start, month_end) = month_bounds(today);
            self.monthly_usage = PeriodUsage::new(month_start, month_end);
        }
    }

    /// Check if any budget limit is exceeded
    pub fn check_budget_exceeded(&mut self) -> bool {
        if !self.config.enabled {
            self.downloads_paused = false;
            return false;
        }

        let weekly_exceeded = self.weekly_usage.exceeds(self.config.weekly_limit_bytes);
        let monthly_exceeded = self.monthly_usage.exceeds(self.config.monthly_limit_bytes);

        self.downloads_paused =
            (weekly_exceeded || monthly_exceeded) && self.config.auto_pause_on_exceed;
        self.downloads_paused
    }

    /// Get the current budget status
    pub fn get_status(&self) -> BudgetStatus {
        if !self.config.enabled {
            return BudgetStatus::Inactive;
        }

        let weekly_pct = self
            .weekly_usage
            .usage_percent(self.config.weekly_limit_bytes);
        let monthly_pct = self
            .monthly_usage
            .usage_percent(self.config.monthly_limit_bytes);

        let max_pct = weekly_pct.max(monthly_pct);

        if max_pct >= 1.0 {
            BudgetStatus::Exceeded
        } else if max_pct >= self.config.warning_threshold {
            BudgetStatus::Warning
        } else {
            BudgetStatus::Active
        }
    }

    /// Get the weekly budget status
    pub fn get_weekly_status(&self) -> BudgetStatus {
        if !self.config.enabled || self.config.weekly_limit_bytes == 0 {
            return BudgetStatus::Inactive;
        }

        let pct = self
            .weekly_usage
            .usage_percent(self.config.weekly_limit_bytes);
        if pct >= 1.0 {
            BudgetStatus::Exceeded
        } else if pct >= self.config.warning_threshold {
            BudgetStatus::Warning
        } else {
            BudgetStatus::Active
        }
    }

    /// Get the monthly budget status
    pub fn get_monthly_status(&self) -> BudgetStatus {
        if !self.config.enabled || self.config.monthly_limit_bytes == 0 {
            return BudgetStatus::Inactive;
        }

        let pct = self
            .monthly_usage
            .usage_percent(self.config.monthly_limit_bytes);
        if pct >= 1.0 {
            BudgetStatus::Exceeded
        } else if pct >= self.config.warning_threshold {
            BudgetStatus::Warning
        } else {
            BudgetStatus::Active
        }
    }

    /// Get a summary of the global budget
    pub fn get_summary(&self) -> GlobalBudgetSummary {
        GlobalBudgetSummary {
            status: self.get_status(),
            weekly: self.get_period_summary(&self.weekly_usage, self.config.weekly_limit_bytes),
            monthly: self.get_period_summary(&self.monthly_usage, self.config.monthly_limit_bytes),
            downloads_paused: self.downloads_paused,
        }
    }

    /// Get a period summary for a specific usage record
    fn get_period_summary(&self, usage: &PeriodUsage, limit_bytes: u64) -> PeriodSummary {
        let pct = usage.usage_percent(limit_bytes);
        let status = if !self.config.enabled || limit_bytes == 0 {
            BudgetStatus::Inactive
        } else if pct >= 1.0 {
            BudgetStatus::Exceeded
        } else if pct >= self.config.warning_threshold {
            BudgetStatus::Warning
        } else {
            BudgetStatus::Active
        };

        PeriodSummary {
            period_start: usage.period_start,
            period_end: usage.period_end,
            bytes_downloaded: usage.bytes_downloaded,
            limit_bytes,
            task_count: usage.task_count,
            usage_percent: pct,
            remaining: usage.remaining(limit_bytes),
            status,
        }
    }

    /// Reset all usage data (manual reset)
    pub fn reset_usage(&mut self) {
        let today = Utc::now().date_naive();
        let (week_start, week_end) = week_bounds(today);
        let (month_start, month_end) = month_bounds(today);

        self.weekly_usage = PeriodUsage::new(week_start, week_end);
        self.monthly_usage = PeriodUsage::new(month_start, month_end);
        self.downloads_paused = false;
    }

    /// Resume downloads after budget was exceeded (manual override)
    pub fn resume_downloads(&mut self) {
        self.downloads_paused = false;
    }

    /// Check if downloads should be paused based on current budget
    pub fn should_pause_downloads(&self) -> bool {
        self.config.enabled && self.downloads_paused
    }
}

impl Default for GlobalBudgetManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate the start and end dates of the week containing the given date
/// Week starts on Monday
pub fn week_bounds(date: NaiveDate) -> (NaiveDate, NaiveDate) {
    let weekday = date.weekday();
    let days_since_monday = match weekday {
        Weekday::Mon => 0,
        Weekday::Tue => 1,
        Weekday::Wed => 2,
        Weekday::Thu => 3,
        Weekday::Fri => 4,
        Weekday::Sat => 5,
        Weekday::Sun => 6,
    };

    let week_start = date - Duration::days(days_since_monday as i64);
    let week_end = week_start + Duration::days(7);

    (week_start, week_end)
}

/// Calculate the start and end dates of the month containing the given date
pub fn month_bounds(date: NaiveDate) -> (NaiveDate, NaiveDate) {
    let month_start = NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap();
    let month_end = if date.month() == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1).unwrap()
    };

    (month_start, month_end)
}

/// Format bytes into a human-readable string
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

/// Save the budget manager state to a file
pub fn save_global_budget_config(manager: &GlobalBudgetManager, path: &Path) -> Result<(), String> {
    let json =
        serde_json::to_string_pretty(manager).map_err(|e| format!("serialize error: {}", e))?;
    std::fs::write(path, json).map_err(|e| format!("write error: {}", e))?;
    Ok(())
}

/// Load the budget manager state from a file
pub fn load_global_budget_config(path: &Path) -> Result<GlobalBudgetManager, String> {
    let json = std::fs::read_to_string(path).map_err(|e| format!("read error: {}", e))?;
    let manager: GlobalBudgetManager =
        serde_json::from_str(&json).map_err(|e| format!("deserialize error: {}", e))?;
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GlobalBudgetConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.weekly_limit_bytes, 0);
        assert_eq!(config.monthly_limit_bytes, 0);
        assert_eq!(config.warning_threshold, 0.8);
        assert!(config.auto_pause_on_exceed);
    }

    #[test]
    fn test_config_builder() {
        let config = GlobalBudgetConfig::new(10 * 1024 * 1024 * 1024, 50 * 1024 * 1024 * 1024)
            .with_warning_threshold(0.9)
            .with_auto_pause(false);

        assert!(config.enabled);
        assert_eq!(config.weekly_limit_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(config.monthly_limit_bytes, 50 * 1024 * 1024 * 1024);
        assert_eq!(config.warning_threshold, 0.9);
        assert!(!config.auto_pause_on_exceed);
    }

    #[test]
    fn test_warning_threshold_clamping() {
        let config = GlobalBudgetConfig::default().with_warning_threshold(1.5);
        assert_eq!(config.warning_threshold, 1.0);

        let config = GlobalBudgetConfig::default().with_warning_threshold(-0.5);
        assert_eq!(config.warning_threshold, 0.0);
    }

    #[test]
    fn test_period_usage_add_bytes() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let mut usage = PeriodUsage::new(today, today + Duration::days(7));

        usage.add_bytes(1024);
        assert_eq!(usage.bytes_downloaded, 1024);

        usage.add_bytes(2048);
        assert_eq!(usage.bytes_downloaded, 3072);
    }

    #[test]
    fn test_period_usage_exceeds() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let mut usage = PeriodUsage::new(today, today + Duration::days(7));

        // Not exceeded with 0 limit (unlimited)
        assert!(!usage.exceeds(0));

        // Not exceeded when under limit
        usage.add_bytes(500);
        assert!(!usage.exceeds(1000));

        // Exceeded when at limit
        usage.add_bytes(500);
        assert!(usage.exceeds(1000));

        // Exceeded when over limit
        usage.add_bytes(100);
        assert!(usage.exceeds(1000));
    }

    #[test]
    fn test_period_usage_remaining() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let mut usage = PeriodUsage::new(today, today + Duration::days(7));

        // Unlimited returns u64::MAX
        assert_eq!(usage.remaining(0), u64::MAX);

        usage.add_bytes(300);
        assert_eq!(usage.remaining(1000), 700);
    }

    #[test]
    fn test_period_usage_percent() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let mut usage = PeriodUsage::new(today, today + Duration::days(7));

        // 0% with unlimited
        assert_eq!(usage.usage_percent(0), 0.0);

        usage.add_bytes(500);
        assert_eq!(usage.usage_percent(1000), 0.5);

        usage.add_bytes(500);
        assert_eq!(usage.usage_percent(1000), 1.0);

        usage.add_bytes(500);
        assert_eq!(usage.usage_percent(1000), 1.5);
    }

    #[test]
    fn test_period_active_and_expired() {
        let start = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let usage = PeriodUsage::new(start, end);

        // Before period
        assert!(!usage.is_active(NaiveDate::from_ymd_opt(2026, 8, 9).unwrap()));

        // Start of period
        assert!(usage.is_active(start));

        // Middle of period
        assert!(usage.is_active(NaiveDate::from_ymd_opt(2026, 8, 13).unwrap()));

        // End of period (exclusive)
        assert!(!usage.is_active(end));

        // Expired
        assert!(usage.is_expired(end));
        assert!(!usage.is_expired(NaiveDate::from_ymd_opt(2026, 8, 16).unwrap()));
    }

    #[test]
    fn test_week_bounds_monday() {
        // Monday Aug 10, 2026
        let date = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let (start, end) = week_bounds(date);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 8, 10).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
    }

    #[test]
    fn test_week_bounds_wednesday() {
        // Wednesday Aug 12, 2026
        let date = NaiveDate::from_ymd_opt(2026, 8, 12).unwrap();
        let (start, end) = week_bounds(date);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 8, 10).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
    }

    #[test]
    fn test_week_bounds_sunday() {
        // Sunday Aug 16, 2026
        let date = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        let (start, end) = week_bounds(date);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 8, 10).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
    }

    #[test]
    fn test_month_bounds_mid_month() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
        let (start, end) = month_bounds(date);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 9, 1).unwrap());
    }

    #[test]
    fn test_month_bounds_december() {
        let date = NaiveDate::from_ymd_opt(2026, 12, 25).unwrap();
        let (start, end) = month_bounds(date);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 12, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2027, 1, 1).unwrap());
    }

    #[test]
    fn test_budget_manager_new() {
        let manager = GlobalBudgetManager::new();
        assert!(!manager.config.enabled);
        assert!(!manager.downloads_paused);
    }

    #[test]
    fn test_budget_manager_disabled() {
        let mut manager = GlobalBudgetManager::new();
        // Disabled manager should not pause
        let exceeded = manager.record_download(1024 * 1024);
        assert!(!exceeded);
        assert!(!manager.should_pause_downloads());
    }

    #[test]
    fn test_budget_manager_weekly_exceeded() {
        let config = GlobalBudgetConfig::new(1000, 0).with_auto_pause(true);
        let mut manager = GlobalBudgetManager::with_config(config);

        // Record under limit
        let exceeded = manager.record_download(500);
        assert!(!exceeded);

        // Record at limit
        let exceeded = manager.record_download(500);
        assert!(exceeded);
        assert!(manager.should_pause_downloads());
    }

    #[test]
    fn test_budget_manager_monthly_exceeded() {
        let config = GlobalBudgetConfig::new(0, 1000).with_auto_pause(true);
        let mut manager = GlobalBudgetManager::with_config(config);

        // Record under limit
        let exceeded = manager.record_download(500);
        assert!(!exceeded);

        // Record at limit
        let exceeded = manager.record_download(500);
        assert!(exceeded);
        assert!(manager.should_pause_downloads());
    }

    #[test]
    fn test_budget_manager_no_auto_pause() {
        let config = GlobalBudgetConfig::new(1000, 0).with_auto_pause(false);
        let mut manager = GlobalBudgetManager::with_config(config);

        let exceeded = manager.record_download(1000);
        assert!(!exceeded); // auto_pause is off
        assert!(!manager.should_pause_downloads());
    }

    #[test]
    fn test_budget_status_inactive() {
        let manager = GlobalBudgetManager::new();
        assert_eq!(manager.get_status(), BudgetStatus::Inactive);
    }

    #[test]
    fn test_budget_status_active() {
        let config = GlobalBudgetConfig::new(10000, 100000);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(1000);
        assert_eq!(manager.get_status(), BudgetStatus::Active);
    }

    #[test]
    fn test_budget_status_warning() {
        let config = GlobalBudgetConfig::new(1000, 0).with_warning_threshold(0.8);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(850);
        assert_eq!(manager.get_status(), BudgetStatus::Warning);
    }

    #[test]
    fn test_budget_status_exceeded() {
        let config = GlobalBudgetConfig::new(1000, 0);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(1000);
        assert_eq!(manager.get_status(), BudgetStatus::Exceeded);
    }

    #[test]
    fn test_reset_usage() {
        let config = GlobalBudgetConfig::new(1000, 0);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(500);
        assert_eq!(manager.weekly_usage.bytes_downloaded, 500);

        manager.reset_usage();
        assert_eq!(manager.weekly_usage.bytes_downloaded, 0);
        assert!(!manager.downloads_paused);
    }

    #[test]
    fn test_resume_downloads() {
        let config = GlobalBudgetConfig::new(1000, 0).with_auto_pause(true);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(1000);
        assert!(manager.downloads_paused);

        manager.resume_downloads();
        assert!(!manager.downloads_paused);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024u64.pow(4)), "1.00 TB");
    }

    #[test]
    fn test_budget_status_display() {
        assert_eq!(BudgetStatus::Inactive.to_string(), "⚪ Inactive");
        assert_eq!(BudgetStatus::Active.to_string(), "🟢 Active");
        assert_eq!(BudgetStatus::Warning.to_string(), "🟡 Warning");
        assert_eq!(BudgetStatus::Exceeded.to_string(), "🔴 Exceeded");
    }

    #[test]
    fn test_summary_format() {
        let config = GlobalBudgetConfig::new(10 * 1024 * 1024, 50 * 1024 * 1024);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(5 * 1024 * 1024);

        let summary = manager.get_summary();
        let formatted = summary.format_summary();

        assert!(formatted.contains("Global Budget Status"));
        assert!(formatted.contains("Weekly"));
        assert!(formatted.contains("Monthly"));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let config = GlobalBudgetConfig::new(1000, 5000).with_warning_threshold(0.9);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(250);

        let json = serde_json::to_string(&manager).unwrap();
        let restored: GlobalBudgetManager = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.config.weekly_limit_bytes, 1000);
        assert_eq!(restored.config.monthly_limit_bytes, 5000);
        assert_eq!(restored.weekly_usage.bytes_downloaded, 250);
    }

    #[test]
    fn test_save_and_load() {
        let config = GlobalBudgetConfig::new(2000, 10000);
        let mut manager = GlobalBudgetManager::with_config(config);
        manager.record_download(500);

        let tmp = std::env::temp_dir().join("test_global_budget.json");
        save_global_budget_config(&manager, &tmp).unwrap();

        let loaded = load_global_budget_config(&tmp).unwrap();
        assert_eq!(loaded.config.weekly_limit_bytes, 2000);
        assert_eq!(loaded.weekly_usage.bytes_downloaded, 500);

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_load_missing_file() {
        let result = load_global_budget_config(Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_record_task() {
        let config = GlobalBudgetConfig::new(10000, 50000);
        let mut manager = GlobalBudgetManager::with_config(config);

        manager.record_task();
        assert_eq!(manager.weekly_usage.task_count, 1);
        assert_eq!(manager.monthly_usage.task_count, 1);

        manager.record_task();
        assert_eq!(manager.weekly_usage.task_count, 2);
        assert_eq!(manager.monthly_usage.task_count, 2);
    }

    #[test]
    fn test_get_weekly_status_unlimited() {
        let config = GlobalBudgetConfig::new(0, 1000);
        let manager = GlobalBudgetManager::with_config(config);
        assert_eq!(manager.get_weekly_status(), BudgetStatus::Inactive);
    }

    #[test]
    fn test_get_monthly_status_unlimited() {
        let config = GlobalBudgetConfig::new(1000, 0);
        let manager = GlobalBudgetManager::with_config(config);
        assert_eq!(manager.get_monthly_status(), BudgetStatus::Inactive);
    }
}
