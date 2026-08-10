//! Download Cost Tracker (Phase 127)
//!
//! Tracks estimated monetary cost of downloads based on configurable cost-per-GB rates.
//! Supports peak/off-peak pricing windows, per-task cost estimation, and aggregate
//! cost reporting over configurable periods.
//!
//! Features:
//! - Configurable cost rate per GB (with peak/off-peak time windows)
//! - Per-task cost tracking based on bytes downloaded
//! - Daily/weekly/monthly aggregate cost summaries
//! - Cost budget alerts when approaching limits
//! - Persistent configuration and usage records

use chrono::{DateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Cost rate for a specific time window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CostRate {
    /// Human-readable name for this rate (e.g., "Peak", "Off-Peak")
    pub name: String,
    /// Cost per GB in the user's currency unit (e.g., dollars, yuan)
    pub cost_per_gb: f64,
    /// Start time of this rate window (inclusive)
    pub start_time: NaiveTime,
    /// End time of this rate window (exclusive)
    pub end_time: NaiveTime,
}

impl CostRate {
    /// Create a flat rate that applies all day.
    pub fn flat(name: impl Into<String>, cost_per_gb: f64) -> Self {
        Self {
            name: name.into(),
            cost_per_gb,
            start_time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        }
    }

    /// Create a time-windowed rate.
    pub fn windowed(
        name: impl Into<String>,
        cost_per_gb: f64,
        start_hour: u32,
        start_min: u32,
        end_hour: u32,
        end_min: u32,
    ) -> Self {
        Self {
            name: name.into(),
            cost_per_gb,
            start_time: NaiveTime::from_hms_opt(start_hour, start_min, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(end_hour, end_min, 0).unwrap(),
        }
    }

    /// Check if a given time falls within this rate window.
    pub fn matches(&self, time: NaiveTime) -> bool {
        if self.start_time <= self.end_time {
            time >= self.start_time && time < self.end_time
        } else {
            // Wraps midnight (e.g., 22:00 - 06:00)
            time >= self.start_time || time < self.end_time
        }
    }
}

/// Configuration for the cost tracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostConfig {
    /// Whether cost tracking is enabled.
    pub enabled: bool,
    /// Currency unit label (e.g., "¥", "$", "CNY")
    pub currency: String,
    /// Cost rate definitions (checked in order; first match wins).
    pub rates: Vec<CostRate>,
    /// Optional monthly budget limit (0 = no limit).
    pub monthly_budget: f64,
    /// Alert threshold percentage (e.g., 0.8 = alert at 80% of budget).
    pub alert_threshold: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 0.0)],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        }
    }
}

/// Cost record for a single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCostRecord {
    /// Task ID.
    pub task_id: String,
    /// Task name (for display).
    pub task_name: String,
    /// Total bytes downloaded (for cost calculation).
    pub bytes_downloaded: u64,
    /// Estimated cost for this task.
    pub cost: f64,
    /// When the task was first tracked.
    pub started_at: DateTime<Utc>,
    /// When the cost was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Daily usage record for aggregate tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DailyCostUsage {
    /// Date string (YYYY-MM-DD).
    pub date: String,
    /// Total bytes downloaded on this day.
    pub bytes_downloaded: u64,
    /// Total cost incurred on this day.
    pub total_cost: f64,
    /// Number of tasks that contributed to this day's cost.
    pub task_count: u32,
}

/// Cost summary for a period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostSummary {
    /// Period label (e.g., "2026-08", "2026-08-11", "weekly").
    pub period: String,
    /// Total bytes downloaded in this period.
    pub total_bytes: u64,
    /// Total estimated cost in this period.
    pub total_cost: f64,
    /// Number of tasks tracked.
    pub task_count: usize,
    /// Currency label.
    pub currency: String,
    /// Monthly budget (0 = no limit).
    pub monthly_budget: f64,
    /// Budget usage percentage (0.0 - 1.0+).
    pub budget_usage_pct: f64,
    /// Whether budget alert threshold is exceeded.
    pub budget_alert: bool,
    /// Average cost per GB in this period.
    pub avg_cost_per_gb: f64,
    /// Peak single-task cost.
    pub peak_task_cost: f64,
    /// Peak single-task name.
    pub peak_task_name: Option<String>,
}

/// The main cost tracker.
#[derive(Debug)]
pub struct CostTracker {
    /// Configuration.
    config: CostConfig,
    /// Per-task cost records.
    task_records: HashMap<String, TaskCostRecord>,
    /// Daily usage records (keyed by date string).
    daily_usage: HashMap<String, DailyCostUsage>,
}

impl CostTracker {
    /// Create a new cost tracker with default config.
    pub fn new() -> Self {
        Self {
            config: CostConfig::default(),
            task_records: HashMap::new(),
            daily_usage: HashMap::new(),
        }
    }

    /// Create with a specific config.
    pub fn with_config(config: CostConfig) -> Self {
        Self {
            config,
            task_records: HashMap::new(),
            daily_usage: HashMap::new(),
        }
    }

    /// Get current config.
    pub fn config(&self) -> &CostConfig {
        &self.config
    }

    /// Update config.
    pub fn set_config(&mut self, config: CostConfig) {
        self.config = config;
    }

    /// Determine the applicable cost rate for a given timestamp.
    pub fn rate_at(&self, time: DateTime<Utc>) -> CostRate {
        let naive_time = time.time();
        for rate in &self.config.rates {
            if rate.matches(naive_time) {
                return rate.clone();
            }
        }
        // Fallback: return last rate or a zero rate
        self.config
            .rates
            .last()
            .cloned()
            .unwrap_or_else(|| CostRate::flat("Fallback", 0.0))
    }

    /// Calculate cost for a given number of bytes at a specific time.
    pub fn cost_for_bytes(&self, bytes: u64, time: DateTime<Utc>) -> f64 {
        let rate = self.rate_at(time);
        let gb = bytes as f64 / 1_073_741_824.0;
        gb * rate.cost_per_gb
    }

    /// Record or update cost for a task.
    pub fn record_task_usage(
        &mut self,
        task_id: &str,
        task_name: &str,
        bytes_downloaded: u64,
        now: DateTime<Utc>,
    ) {
        if !self.config.enabled {
            return;
        }

        let cost = self.cost_for_bytes(bytes_downloaded, now);
        let date_str = now.format("%Y-%m-%d").to_string();

        // Update task record
        if let Some(existing) = self.task_records.get_mut(task_id) {
            existing.bytes_downloaded = bytes_downloaded;
            existing.cost = cost;
            existing.updated_at = now;
        } else {
            self.task_records.insert(
                task_id.to_string(),
                TaskCostRecord {
                    task_id: task_id.to_string(),
                    task_name: task_name.to_string(),
                    bytes_downloaded,
                    cost,
                    started_at: now,
                    updated_at: now,
                },
            );
        }

        // Update daily usage
        let daily = self
            .daily_usage
            .entry(date_str.clone())
            .or_insert_with(|| DailyCostUsage {
                date: date_str,
                ..Default::default()
            });
        // Recalculate daily totals from task records updated today
        daily.bytes_downloaded = self
            .task_records
            .values()
            .filter(|r| r.updated_at.format("%Y-%m-%d").to_string() == daily.date)
            .map(|r| r.bytes_downloaded)
            .sum();
        daily.total_cost = self
            .task_records
            .values()
            .filter(|r| r.updated_at.format("%Y-%m-%d").to_string() == daily.date)
            .map(|r| r.cost)
            .sum();
        daily.task_count = self
            .task_records
            .values()
            .filter(|r| r.updated_at.format("%Y-%m-%d").to_string() == daily.date)
            .count() as u32;
    }

    /// Get cost record for a specific task.
    pub fn get_task_cost(&self, task_id: &str) -> Option<&TaskCostRecord> {
        self.task_records.get(task_id)
    }

    /// Remove a task's cost record (e.g., when task is deleted).
    pub fn remove_task(&mut self, task_id: &str) -> Option<TaskCostRecord> {
        self.task_records.remove(task_id)
    }

    /// Get all task cost records.
    pub fn all_task_records(&self) -> &HashMap<String, TaskCostRecord> {
        &self.task_records
    }

    /// Get daily usage records.
    pub fn daily_usage(&self) -> &HashMap<String, DailyCostUsage> {
        &self.daily_usage
    }

    /// Generate a cost summary for a specific date.
    pub fn summary_for_date(&self, date: &str) -> CostSummary {
        let records: Vec<&TaskCostRecord> = self
            .task_records
            .values()
            .filter(|r| r.updated_at.format("%Y-%m-%d").to_string() == date)
            .collect();

        self.build_summary(date.to_string(), &records)
    }

    /// Generate a cost summary for a specific month (YYYY-MM).
    pub fn summary_for_month(&self, year_month: &str) -> CostSummary {
        let records: Vec<&TaskCostRecord> = self
            .task_records
            .values()
            .filter(|r| r.updated_at.format("%Y-%m").to_string() == year_month)
            .collect();

        self.build_summary(year_month.to_string(), &records)
    }

    /// Generate a cost summary for the current month.
    pub fn summary_current_month(&self) -> CostSummary {
        let now = Utc::now();
        let year_month = now.format("%Y-%m").to_string();
        self.summary_for_month(&year_month)
    }

    /// Generate a cost summary for all tracked data.
    pub fn summary_all(&self) -> CostSummary {
        let records: Vec<&TaskCostRecord> = self.task_records.values().collect();
        self.build_summary("all-time".to_string(), &records)
    }

    /// Build a summary from a set of task records.
    fn build_summary(&self, period: String, records: &[&TaskCostRecord]) -> CostSummary {
        let total_bytes: u64 = records.iter().map(|r| r.bytes_downloaded).sum();
        let total_cost: f64 = records.iter().map(|r| r.cost).sum();
        let task_count = records.len();

        let (peak_task_cost, peak_task_name) = records
            .iter()
            .max_by(|a, b| {
                a.cost
                    .partial_cmp(&b.cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|r| (r.cost, Some(r.task_name.clone())))
            .unwrap_or((0.0, None));

        let avg_cost_per_gb = if total_bytes > 0 {
            total_cost / (total_bytes as f64 / 1_073_741_824.0)
        } else {
            0.0
        };

        let budget_usage_pct = if self.config.monthly_budget > 0.0 {
            total_cost / self.config.monthly_budget
        } else {
            0.0
        };

        let budget_alert =
            self.config.monthly_budget > 0.0 && budget_usage_pct >= self.config.alert_threshold;

        CostSummary {
            period,
            total_bytes,
            total_cost,
            task_count,
            currency: self.config.currency.clone(),
            monthly_budget: self.config.monthly_budget,
            budget_usage_pct,
            budget_alert,
            avg_cost_per_gb,
            peak_task_cost,
            peak_task_name,
        }
    }

    /// Check if the current month's cost exceeds the budget alert threshold.
    pub fn is_over_budget_alert(&self) -> bool {
        let summary = self.summary_current_month();
        summary.budget_alert
    }

    /// Format a cost value with the configured currency.
    pub fn format_cost(&self, cost: f64) -> String {
        format!("{}{:.2}", self.config.currency, cost)
    }

    /// Format a cost summary for display.
    pub fn format_summary(&self, summary: &CostSummary) -> String {
        let mut lines = Vec::new();
        lines.push(format!("📊 Cost Summary ({})", summary.period));
        lines.push(format!(
            "  Total: {} ({:.2} GB)",
            self.format_cost(summary.total_cost),
            summary.total_bytes as f64 / 1_073_741_824.0
        ));
        lines.push(format!("  Tasks: {}", summary.task_count));
        lines.push(format!(
            "  Avg cost/GB: {}",
            self.format_cost(summary.avg_cost_per_gb)
        ));
        if let Some(ref name) = summary.peak_task_name {
            lines.push(format!(
                "  Peak: {} ({})",
                name,
                self.format_cost(summary.peak_task_cost)
            ));
        }
        if summary.monthly_budget > 0.0 {
            lines.push(format!(
                "  Budget: {} / {} ({:.0}%)",
                self.format_cost(summary.total_cost),
                self.format_cost(summary.monthly_budget),
                summary.budget_usage_pct * 100.0
            ));
            if summary.budget_alert {
                lines.push("  ⚠️  Budget alert threshold exceeded!".to_string());
            }
        }
        lines.join("\n")
    }

    /// Clear all task records and daily usage.
    pub fn clear(&mut self) {
        self.task_records.clear();
        self.daily_usage.clear();
    }

    /// Clear only daily usage records older than the given number of days.
    pub fn prune_daily_usage(&mut self, keep_days: u32) {
        let cutoff = Utc::now() - chrono::Duration::days(keep_days as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d").to_string();
        self.daily_usage
            .retain(|date, _| date.as_str() >= cutoff_str.as_str());
    }

    /// Save configuration to disk.
    pub fn save_config(&self, path: &Path) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    /// Load configuration from disk.
    pub fn load_config(path: &Path) -> Result<CostConfig, std::io::Error> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(std::io::Error::other)
    }

    /// Save full state (config + records + daily usage) to disk.
    pub fn save_state(&self, path: &Path) -> Result<(), std::io::Error> {
        let state = CostState {
            config: self.config.clone(),
            task_records: self.task_records.clone(),
            daily_usage: self.daily_usage.clone(),
        };
        let json = serde_json::to_string_pretty(&state).map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    /// Load full state from disk.
    pub fn load_state(path: &Path) -> Result<Self, std::io::Error> {
        let json = std::fs::read_to_string(path)?;
        let state: CostState = serde_json::from_str(&json).map_err(std::io::Error::other)?;
        Ok(Self {
            config: state.config,
            task_records: state.task_records,
            daily_usage: state.daily_usage,
        })
    }
}

impl Default for CostTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable full state for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CostState {
    config: CostConfig,
    task_records: HashMap<String, TaskCostRecord>,
    daily_usage: HashMap<String, DailyCostUsage>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_time(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 11, h, m, 0).unwrap()
    }

    #[test]
    fn test_cost_rate_flat() {
        let rate = CostRate::flat("Default", 0.50);
        assert!(rate.matches(NaiveTime::from_hms_opt(0, 0, 0).unwrap()));
        assert!(rate.matches(NaiveTime::from_hms_opt(12, 0, 0).unwrap()));
        assert!(rate.matches(NaiveTime::from_hms_opt(23, 59, 0).unwrap()));
    }

    #[test]
    fn test_cost_rate_windowed() {
        let peak = CostRate::windowed("Peak", 1.0, 8, 0, 22, 0);
        assert!(peak.matches(NaiveTime::from_hms_opt(8, 0, 0).unwrap()));
        assert!(peak.matches(NaiveTime::from_hms_opt(15, 30, 0).unwrap()));
        assert!(!peak.matches(NaiveTime::from_hms_opt(22, 0, 0).unwrap()));
        assert!(!peak.matches(NaiveTime::from_hms_opt(3, 0, 0).unwrap()));
    }

    #[test]
    fn test_cost_rate_wraps_midnight() {
        let off_peak = CostRate::windowed("Night", 0.25, 22, 0, 6, 0);
        assert!(off_peak.matches(NaiveTime::from_hms_opt(23, 0, 0).unwrap()));
        assert!(off_peak.matches(NaiveTime::from_hms_opt(0, 0, 0).unwrap()));
        assert!(off_peak.matches(NaiveTime::from_hms_opt(5, 59, 0).unwrap()));
        assert!(!off_peak.matches(NaiveTime::from_hms_opt(6, 0, 0).unwrap()));
        assert!(!off_peak.matches(NaiveTime::from_hms_opt(12, 0, 0).unwrap()));
    }

    #[test]
    fn test_cost_for_bytes() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 10.0)],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        };
        let tracker = CostTracker::with_config(config);
        let time = make_time(12, 0);
        // 1 GB at $10/GB = $10
        let cost = tracker.cost_for_bytes(1_073_741_824, time);
        assert!((cost - 10.0).abs() < 0.001);
        // 500 MB at $10/GB = $5
        let cost = tracker.cost_for_bytes(536_870_912, time);
        assert!((cost - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_rate_selection_by_time() {
        let config = CostConfig {
            enabled: true,
            currency: "¥".to_string(),
            rates: vec![
                CostRate::windowed("Peak", 2.0, 8, 0, 22, 0),
                CostRate::windowed("Off-Peak", 0.5, 22, 0, 8, 0),
            ],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        };
        let tracker = CostTracker::with_config(config);

        // Peak time
        let peak_time = make_time(12, 0);
        let rate = tracker.rate_at(peak_time);
        assert_eq!(rate.name, "Peak");
        assert!((rate.cost_per_gb - 2.0).abs() < 0.001);

        // Off-peak time
        let off_peak_time = make_time(23, 30);
        let rate = tracker.rate_at(off_peak_time);
        assert_eq!(rate.name, "Off-Peak");
        assert!((rate.cost_per_gb - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_record_task_usage() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 10.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        tracker.record_task_usage("task1", "big_file.zip", 1_073_741_824, now); // 1 GB

        let record = tracker.get_task_cost("task1").unwrap();
        assert_eq!(record.task_name, "big_file.zip");
        assert_eq!(record.bytes_downloaded, 1_073_741_824);
        assert!((record.cost - 10.0).abs() < 0.001);

        // Daily usage should be updated
        let daily = tracker.daily_usage().get("2026-08-11").unwrap();
        assert_eq!(daily.bytes_downloaded, 1_073_741_824);
        assert!((daily.total_cost - 10.0).abs() < 0.001);
        assert_eq!(daily.task_count, 1);
    }

    #[test]
    fn test_disabled_tracker_no_records() {
        let config = CostConfig {
            enabled: false,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        tracker.record_task_usage("task1", "file.zip", 1_073_741_824, now);
        assert!(tracker.get_task_cost("task1").is_none());
        assert!(tracker.daily_usage().is_empty());
    }

    #[test]
    fn test_remove_task() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("Default", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        tracker.record_task_usage("task1", "file.zip", 100, now);
        assert!(tracker.get_task_cost("task1").is_some());

        let removed = tracker.remove_task("task1");
        assert!(removed.is_some());
        assert!(tracker.get_task_cost("task1").is_none());
    }

    #[test]
    fn test_summary_for_date() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 10.0)],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, now); // 1 GB = $10
        tracker.record_task_usage("t2", "b.zip", 2_147_483_648, now); // 2 GB = $20

        let summary = tracker.summary_for_date("2026-08-11");
        assert_eq!(summary.task_count, 2);
        assert_eq!(summary.total_bytes, 3_221_225_472); // 3 GB
        assert!((summary.total_cost - 30.0).abs() < 0.001);
        assert!((summary.avg_cost_per_gb - 10.0).abs() < 0.001);
        assert_eq!(summary.peak_task_name.as_deref(), Some("b.zip"));
        assert!((summary.peak_task_cost - 20.0).abs() < 0.001);
    }

    #[test]
    fn test_budget_alert() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 10.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        // 8 GB = $80, which is 80% of $100 budget
        tracker.record_task_usage("t1", "big.zip", 8_589_934_592, now);

        let summary = tracker.summary_current_month();
        assert!(summary.budget_alert);
        assert!((summary.budget_usage_pct - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_no_budget_alert_under_threshold() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 1.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        // 100 MB = ~$0.10, well under 80% of $100
        tracker.record_task_usage("t1", "small.zip", 104_857_600, now);

        let summary = tracker.summary_current_month();
        assert!(!summary.budget_alert);
    }

    #[test]
    fn test_format_cost() {
        let config = CostConfig {
            currency: "¥".to_string(),
            ..Default::default()
        };
        let tracker = CostTracker::with_config(config);
        assert_eq!(tracker.format_cost(12.50), "¥12.50");
    }

    #[test]
    fn test_format_summary() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 10.0)],
            monthly_budget: 50.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "file.zip", 1_073_741_824, now);

        let summary = tracker.summary_for_date("2026-08-11");
        let formatted = tracker.format_summary(&summary);
        assert!(formatted.contains("Cost Summary"));
        assert!(formatted.contains("$10.00"));
        assert!(formatted.contains("Budget"));
    }

    #[test]
    fn test_clear() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("Default", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        tracker.record_task_usage("t1", "a.zip", 100, now);
        tracker.record_task_usage("t2", "b.zip", 200, now);

        assert_eq!(tracker.all_task_records().len(), 2);
        tracker.clear();
        assert_eq!(tracker.all_task_records().len(), 0);
        assert!(tracker.daily_usage().is_empty());
    }

    #[test]
    fn test_persistence_roundtrip() {
        let config = CostConfig {
            enabled: true,
            currency: "€".to_string(),
            rates: vec![
                CostRate::windowed("Peak", 2.0, 8, 0, 22, 0),
                CostRate::windowed("Off-Peak", 0.5, 22, 0, 8, 0),
            ],
            monthly_budget: 200.0,
            alert_threshold: 0.9,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "file.zip", 1_073_741_824, now);

        let tmp = std::env::temp_dir().join("cost_tracker_test.json");
        tracker.save_state(&tmp).unwrap();

        let loaded = CostTracker::load_state(&tmp).unwrap();
        assert_eq!(loaded.config().currency, "€");
        assert_eq!(loaded.all_task_records().len(), 1);
        let record = loaded.get_task_cost("t1").unwrap();
        assert_eq!(record.task_name, "file.zip");
        assert!((record.cost - 2.0).abs() < 0.001); // Peak rate at 12:00

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_config_persistence() {
        let config = CostConfig {
            enabled: true,
            currency: "£".to_string(),
            rates: vec![CostRate::flat("Default", 5.0)],
            monthly_budget: 500.0,
            alert_threshold: 0.75,
        };
        let tracker = CostTracker::with_config(config);

        let tmp = std::env::temp_dir().join("cost_config_test.json");
        tracker.save_config(&tmp).unwrap();

        let loaded = CostTracker::load_config(&tmp).unwrap();
        assert_eq!(loaded.currency, "£");
        assert!((loaded.monthly_budget - 500.0).abs() < 0.001);
        assert!((loaded.alert_threshold - 0.75).abs() < 0.001);

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_update_existing_task_record() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("Default", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        tracker.record_task_usage("t1", "file.zip", 1_073_741_824, now);
        assert!((tracker.get_task_cost("t1").unwrap().cost - 10.0).abs() < 0.001);

        // Update with more bytes
        tracker.record_task_usage("t1", "file.zip", 2_147_483_648, now);
        assert!((tracker.get_task_cost("t1").unwrap().cost - 20.0).abs() < 0.001);

        // Should still be only 1 record
        assert_eq!(tracker.all_task_records().len(), 1);
    }

    #[test]
    fn test_summary_all_time() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 1.0)],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let t1 = make_time(10, 0);
        let t2 = make_time(14, 0);

        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, t1);
        tracker.record_task_usage("t2", "b.zip", 1_073_741_824, t2);

        let summary = tracker.summary_all();
        assert_eq!(summary.period, "all-time");
        assert_eq!(summary.task_count, 2);
        assert_eq!(summary.total_bytes, 2_147_483_648);
    }

    #[test]
    fn test_empty_summary() {
        let tracker = CostTracker::new();
        let summary = tracker.summary_all();
        assert_eq!(summary.task_count, 0);
        assert_eq!(summary.total_bytes, 0);
        assert!((summary.total_cost - 0.0).abs() < 0.001);
        assert!((summary.avg_cost_per_gb - 0.0).abs() < 0.001);
        assert!(summary.peak_task_name.is_none());
    }

    #[test]
    fn test_prune_daily_usage() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("Default", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);

        // Manually insert old daily usage
        tracker.daily_usage.insert(
            "2020-01-01".to_string(),
            DailyCostUsage {
                date: "2020-01-01".to_string(),
                bytes_downloaded: 100,
                total_cost: 0.01,
                task_count: 1,
            },
        );
        tracker.daily_usage.insert(
            "2099-12-31".to_string(),
            DailyCostUsage {
                date: "2099-12-31".to_string(),
                bytes_downloaded: 200,
                total_cost: 0.02,
                task_count: 1,
            },
        );

        tracker.prune_daily_usage(365);
        // Old entry should be pruned, future entry should remain
        assert!(!tracker.daily_usage().contains_key("2020-01-01"));
        assert!(tracker.daily_usage().contains_key("2099-12-31"));
    }

    #[test]
    fn test_is_over_budget_alert() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("Default", 100.0)],
            monthly_budget: 50.0,
            alert_threshold: 0.5,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        // Not over budget yet
        assert!(!tracker.is_over_budget_alert());

        // 0.5 GB at $100/GB = $50, which is 100% of $50 budget
        tracker.record_task_usage("t1", "huge.zip", 536_870_912, now);
        assert!(tracker.is_over_budget_alert());
    }
}
