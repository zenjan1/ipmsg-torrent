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

        let summary = tracker.summary_for_date("2026-08-11");
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

        let summary = tracker.summary_for_date("2026-08-11");
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
        // Use summary_for_date instead of is_over_budget_alert since test data is from 2026-08-11
        let summary = tracker.summary_for_date("2026-08-11");
        assert!(summary.budget_alert);
    }

    // === Phase 253: Comprehensive Test Coverage ===

    // -- CostRate serde --
    #[test]
    fn cost_rate_serde_roundtrip() {
        let rate = CostRate::flat("Default", 1.5);
        let json = serde_json::to_string(&rate).unwrap();
        let loaded: CostRate = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.name, "Default");
        assert!((loaded.cost_per_gb - 1.5).abs() < f64::EPSILON);
        assert_eq!(loaded.start_time, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        assert_eq!(
            loaded.end_time,
            NaiveTime::from_hms_opt(23, 59, 59).unwrap()
        );
    }

    #[test]
    fn cost_rate_serde_windowed_roundtrip() {
        let rate = CostRate::windowed("Peak", 2.5, 8, 30, 22, 0);
        let json = serde_json::to_string(&rate).unwrap();
        let loaded: CostRate = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.name, "Peak");
        assert_eq!(
            loaded.start_time,
            NaiveTime::from_hms_opt(8, 30, 0).unwrap()
        );
        assert_eq!(loaded.end_time, NaiveTime::from_hms_opt(22, 0, 0).unwrap());
    }

    #[test]
    fn cost_rate_serde_extra_fields_ignored() {
        let json = r#"{"name":"X","cost_per_gb":1.0,"start_time":"00:00:00","end_time":"23:59:59","extra":"ignored"}"#;
        let loaded: CostRate = serde_json::from_str(json).unwrap();
        assert_eq!(loaded.name, "X");
    }

    #[test]
    fn cost_rate_serde_pretty() {
        let rate = CostRate::flat("Test", 3.0);
        let pretty = serde_json::to_string_pretty(&rate).unwrap();
        let loaded: CostRate = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.name, "Test");
    }

    // -- CostRate traits --
    #[test]
    fn cost_rate_clone() {
        let rate = CostRate::flat("A", 1.0);
        let cloned = rate.clone();
        assert_eq!(cloned.name, "A");
        assert!((cloned.cost_per_gb - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_rate_clone_independence() {
        let mut rate = CostRate::flat("A", 1.0);
        let cloned = rate.clone();
        rate.name = "B".into();
        assert_eq!(cloned.name, "A");
    }

    #[test]
    fn cost_rate_debug() {
        let rate = CostRate::flat("Debug", 0.5);
        let debug = format!("{:?}", rate);
        assert!(debug.contains("Debug"));
        assert!(debug.contains("0.5"));
    }

    #[test]
    fn cost_rate_partial_eq() {
        let a = CostRate::flat("X", 1.0);
        let b = CostRate::flat("X", 1.0);
        assert_eq!(a, b);
    }

    // -- CostRate matches boundary --
    #[test]
    fn cost_rate_matches_start_inclusive() {
        let rate = CostRate::windowed("P", 1.0, 8, 0, 22, 0);
        assert!(rate.matches(NaiveTime::from_hms_opt(8, 0, 0).unwrap()));
    }

    #[test]
    fn cost_rate_matches_end_exclusive() {
        let rate = CostRate::windowed("P", 1.0, 8, 0, 22, 0);
        assert!(!rate.matches(NaiveTime::from_hms_opt(22, 0, 0).unwrap()));
    }

    #[test]
    fn cost_rate_matches_one_second_before_end() {
        let rate = CostRate::windowed("P", 1.0, 8, 0, 22, 0);
        assert!(rate.matches(NaiveTime::from_hms_opt(21, 59, 59).unwrap()));
    }

    #[test]
    fn cost_rate_flat_matches_all_day() {
        let rate = CostRate::flat("All", 0.5);
        for h in 0..24 {
            assert!(rate.matches(NaiveTime::from_hms_opt(h, 0, 0).unwrap()));
        }
    }

    // -- CostConfig --
    #[test]
    fn cost_config_default_values() {
        let config = CostConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.currency, "$");
        assert_eq!(config.rates.len(), 1);
        assert!((config.monthly_budget - 0.0).abs() < f64::EPSILON);
        assert!((config.alert_threshold - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_config_serde_roundtrip() {
        let config = CostConfig {
            enabled: true,
            currency: "¥".to_string(),
            rates: vec![CostRate::flat("Peak", 5.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.9,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: CostConfig = serde_json::from_str(&json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.currency, "¥");
        assert!((loaded.monthly_budget - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":false,"currency":"$","rates":[],"monthly_budget":0.0,"alert_threshold":0.8,"extra":"field"}"#;
        let loaded: CostConfig = serde_json::from_str(json).unwrap();
        assert!(!loaded.enabled);
    }

    #[test]
    fn cost_config_clone() {
        let config = CostConfig {
            enabled: true,
            currency: "€".to_string(),
            rates: vec![CostRate::flat("A", 1.0)],
            monthly_budget: 50.0,
            alert_threshold: 0.7,
        };
        let cloned = config.clone();
        assert_eq!(cloned.currency, "€");
        assert_eq!(cloned.rates.len(), 1);
    }

    #[test]
    fn cost_config_clone_independence() {
        let mut config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("A", 1.0)],
            monthly_budget: 50.0,
            alert_threshold: 0.7,
        };
        let cloned = config.clone();
        config.currency = "€".to_string();
        assert_eq!(cloned.currency, "$");
    }

    #[test]
    fn cost_config_debug() {
        let config = CostConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("CostConfig"));
        assert!(debug.contains("enabled"));
    }

    // -- TaskCostRecord --
    #[test]
    fn task_cost_record_serde_roundtrip() {
        let now = make_time(12, 0);
        let record = TaskCostRecord {
            task_id: "t1".to_string(),
            task_name: "file.zip".to_string(),
            bytes_downloaded: 1024,
            cost: 0.01,
            started_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&record).unwrap();
        let loaded: TaskCostRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, "t1");
        assert_eq!(loaded.bytes_downloaded, 1024);
    }

    #[test]
    fn task_cost_record_clone() {
        let now = make_time(12, 0);
        let record = TaskCostRecord {
            task_id: "t1".to_string(),
            task_name: "f.zip".to_string(),
            bytes_downloaded: 100,
            cost: 0.0,
            started_at: now,
            updated_at: now,
        };
        let cloned = record.clone();
        assert_eq!(cloned.task_id, "t1");
        assert_eq!(cloned.task_name, "f.zip");
    }

    #[test]
    fn task_cost_record_debug() {
        let now = make_time(12, 0);
        let record = TaskCostRecord {
            task_id: "t1".to_string(),
            task_name: "file.zip".to_string(),
            bytes_downloaded: 0,
            cost: 0.0,
            started_at: now,
            updated_at: now,
        };
        let debug = format!("{:?}", record);
        assert!(debug.contains("t1"));
        assert!(debug.contains("file.zip"));
    }

    // -- DailyCostUsage --
    #[test]
    fn daily_cost_usage_default() {
        let usage = DailyCostUsage::default();
        assert_eq!(usage.date, "");
        assert_eq!(usage.bytes_downloaded, 0);
        assert!((usage.total_cost - 0.0).abs() < f64::EPSILON);
        assert_eq!(usage.task_count, 0);
    }

    #[test]
    fn daily_cost_usage_serde_roundtrip() {
        let usage = DailyCostUsage {
            date: "2026-08-11".to_string(),
            bytes_downloaded: 1_073_741_824,
            total_cost: 5.5,
            task_count: 3,
        };
        let json = serde_json::to_string(&usage).unwrap();
        let loaded: DailyCostUsage = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.date, "2026-08-11");
        assert_eq!(loaded.bytes_downloaded, 1_073_741_824);
        assert_eq!(loaded.task_count, 3);
    }

    #[test]
    fn daily_cost_usage_clone_debug() {
        let usage = DailyCostUsage {
            date: "2026-01-01".to_string(),
            bytes_downloaded: 100,
            total_cost: 0.01,
            task_count: 1,
        };
        let cloned = usage.clone();
        assert_eq!(cloned.date, "2026-01-01");
        let debug = format!("{:?}", usage);
        assert!(debug.contains("2026-01-01"));
    }

    // -- CostSummary --
    #[test]
    fn cost_summary_serde_roundtrip() {
        let summary = CostSummary {
            period: "2026-08".to_string(),
            total_bytes: 1_073_741_824,
            total_cost: 10.0,
            task_count: 5,
            currency: "$".to_string(),
            monthly_budget: 100.0,
            budget_usage_pct: 0.1,
            budget_alert: false,
            avg_cost_per_gb: 10.0,
            peak_task_cost: 5.0,
            peak_task_name: Some("big.zip".to_string()),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: CostSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.period, "2026-08");
        assert_eq!(loaded.task_count, 5);
        assert_eq!(loaded.peak_task_name.as_deref(), Some("big.zip"));
    }

    #[test]
    fn cost_summary_clone_debug() {
        let summary = CostSummary {
            period: "test".to_string(),
            total_bytes: 0,
            total_cost: 0.0,
            task_count: 0,
            currency: "$".to_string(),
            monthly_budget: 0.0,
            budget_usage_pct: 0.0,
            budget_alert: false,
            avg_cost_per_gb: 0.0,
            peak_task_cost: 0.0,
            peak_task_name: None,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.period, "test");
        let debug = format!("{:?}", summary);
        assert!(debug.contains("test"));
    }

    // -- CostTracker new/default --
    #[test]
    fn cost_tracker_new_default_eq() {
        let new = CostTracker::new();
        let default = CostTracker::default();
        assert_eq!(new.config().currency, default.config().currency);
        assert_eq!(
            new.all_task_records().len(),
            default.all_task_records().len()
        );
    }

    #[test]
    fn cost_tracker_new_empty_state() {
        let tracker = CostTracker::new();
        assert!(!tracker.config().enabled);
        assert_eq!(tracker.all_task_records().len(), 0);
        assert!(tracker.daily_usage().is_empty());
    }

    #[test]
    fn cost_tracker_with_config() {
        let config = CostConfig {
            enabled: true,
            currency: "CHF".to_string(),
            rates: vec![CostRate::flat("Standard", 3.0)],
            monthly_budget: 200.0,
            alert_threshold: 0.75,
        };
        let tracker = CostTracker::with_config(config);
        assert!(tracker.config().enabled);
        assert_eq!(tracker.config().currency, "CHF");
    }

    #[test]
    fn cost_tracker_set_config() {
        let mut tracker = CostTracker::new();
        assert_eq!(tracker.config().currency, "$");
        let mut new_config = CostConfig::default();
        new_config.currency = "€".to_string();
        tracker.set_config(new_config);
        assert_eq!(tracker.config().currency, "€");
    }

    // -- rate_at edge cases --
    #[test]
    fn rate_at_empty_rates_returns_fallback() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        };
        let tracker = CostTracker::with_config(config);
        let rate = tracker.rate_at(make_time(12, 0));
        assert_eq!(rate.name, "Fallback");
        assert!((rate.cost_per_gb - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rate_at_first_match_wins() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("First", 1.0), CostRate::flat("Second", 2.0)],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
        };
        let tracker = CostTracker::with_config(config);
        let rate = tracker.rate_at(make_time(12, 0));
        assert_eq!(rate.name, "First");
    }

    // -- cost_for_bytes edge cases --
    #[test]
    fn cost_for_bytes_zero() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let tracker = CostTracker::with_config(config);
        let cost = tracker.cost_for_bytes(0, make_time(12, 0));
        assert!((cost - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_for_bytes_large_value() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            ..Default::default()
        };
        let tracker = CostTracker::with_config(config);
        // 1 TB = 1024 GB = $1024
        let cost = tracker.cost_for_bytes(1_099_511_627_776, make_time(12, 0));
        assert!((cost - 1024.0).abs() < 0.001);
    }

    #[test]
    fn cost_for_bytes_zero_rate() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("Free", 0.0)],
            ..Default::default()
        };
        let tracker = CostTracker::with_config(config);
        let cost = tracker.cost_for_bytes(1_073_741_824, make_time(12, 0));
        assert!((cost - 0.0).abs() < f64::EPSILON);
    }

    // -- record_task_usage edge cases --
    #[test]
    fn record_task_usage_zero_bytes() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "empty.zip", 0, now);
        let record = tracker.get_task_cost("t1").unwrap();
        assert_eq!(record.bytes_downloaded, 0);
        assert!((record.cost - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn record_task_usage_unicode_task_name() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "中文文件.zip", 1_073_741_824, now);
        let record = tracker.get_task_cost("t1").unwrap();
        assert_eq!(record.task_name, "中文文件.zip");
    }

    #[test]
    fn record_task_usage_emoji_task_name() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "🔥hot.zip", 1_073_741_824, now);
        let record = tracker.get_task_cost("t1").unwrap();
        assert_eq!(record.task_name, "🔥hot.zip");
    }

    #[test]
    fn record_task_usage_unicode_task_id() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("任务-001", "file.zip", 100, now);
        assert!(tracker.get_task_cost("任务-001").is_some());
    }

    #[test]
    fn record_task_usage_multiple_tasks_same_day() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, now);
        tracker.record_task_usage("t2", "b.zip", 2_147_483_648, now);
        tracker.record_task_usage("t3", "c.zip", 536_870_912, now);
        let daily = tracker.daily_usage().get("2026-08-11").unwrap();
        assert_eq!(daily.task_count, 3);
        assert_eq!(
            daily.bytes_downloaded,
            1_073_741_824 + 2_147_483_648 + 536_870_912
        );
    }

    #[test]
    fn record_task_usage_different_dates() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let t1 = make_time(12, 0);
        let t2 = Utc.with_ymd_and_hms(2026, 8, 12, 12, 0, 0).unwrap();
        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, t1);
        tracker.record_task_usage("t2", "b.zip", 1_073_741_824, t2);
        assert_eq!(tracker.daily_usage().len(), 2);
        assert!(tracker.daily_usage().contains_key("2026-08-11"));
        assert!(tracker.daily_usage().contains_key("2026-08-12"));
    }

    // -- remove_task edge cases --
    #[test]
    fn remove_task_nonexistent() {
        let mut tracker = CostTracker::new();
        assert!(tracker.remove_task("nonexistent").is_none());
    }

    #[test]
    fn remove_task_idempotent() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "f.zip", 100, now);
        assert!(tracker.remove_task("t1").is_some());
        assert!(tracker.remove_task("t1").is_none());
    }

    // -- get_task_cost --
    #[test]
    fn get_task_cost_nonexistent() {
        let tracker = CostTracker::new();
        assert!(tracker.get_task_cost("nonexistent").is_none());
    }

    // -- summary edge cases --
    #[test]
    fn summary_for_date_empty() {
        let tracker = CostTracker::new();
        let summary = tracker.summary_for_date("2026-08-11");
        assert_eq!(summary.task_count, 0);
        assert_eq!(summary.total_bytes, 0);
        assert!((summary.total_cost - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn summary_for_month() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, now);
        let summary = tracker.summary_for_month("2026-08");
        assert_eq!(summary.period, "2026-08");
        assert_eq!(summary.task_count, 1);
    }

    #[test]
    fn summary_for_month_empty() {
        let tracker = CostTracker::new();
        let summary = tracker.summary_for_month("2026-08");
        assert_eq!(summary.task_count, 0);
    }

    #[test]
    fn summary_for_month_no_match() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, now);
        let summary = tracker.summary_for_month("2025-01");
        assert_eq!(summary.task_count, 0);
    }

    // -- budget edge cases --
    #[test]
    fn budget_zero_monthly_budget_no_alert() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 100.0)],
            monthly_budget: 0.0,
            alert_threshold: 0.8,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "big.zip", 10_995_116_277_776, now);
        let summary = tracker.summary_all();
        assert!(!summary.budget_alert);
        assert!((summary.budget_usage_pct - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn budget_exact_threshold_boundary() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        // Exactly 80% of $100 = $80 → 8 GB
        tracker.record_task_usage("t1", "exact.zip", 8_589_934_592, now);
        let summary = tracker.summary_for_date("2026-08-11");
        assert!(summary.budget_alert);
    }

    #[test]
    fn budget_just_under_threshold() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        // Just under 80%: 7.9 GB = $79
        tracker.record_task_usage("t1", "under.zip", 8_482_560_410, now);
        let summary = tracker.summary_for_date("2026-08-11");
        // $79 / $100 = 0.79, under 0.8 threshold
        assert!(!summary.budget_alert);
    }

    #[test]
    fn budget_over_100_percent() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            monthly_budget: 50.0,
            alert_threshold: 0.8,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        // 10 GB = $100, which is 200% of $50 budget
        tracker.record_task_usage("t1", "over.zip", 10_737_418_240, now);
        let summary = tracker.summary_for_date("2026-08-11");
        assert!(summary.budget_alert);
        assert!((summary.budget_usage_pct - 2.0).abs() < 0.01);
    }

    // -- format_cost edge cases --
    #[test]
    fn format_cost_zero() {
        let tracker = CostTracker::with_config(CostConfig {
            currency: "$".to_string(),
            ..Default::default()
        });
        assert_eq!(tracker.format_cost(0.0), "$0.00");
    }

    #[test]
    fn format_cost_negative() {
        let tracker = CostTracker::with_config(CostConfig {
            currency: "$".to_string(),
            ..Default::default()
        });
        assert_eq!(tracker.format_cost(-5.0), "$-5.00");
    }

    #[test]
    fn format_cost_unicode_currency() {
        let tracker = CostTracker::with_config(CostConfig {
            currency: "元".to_string(),
            ..Default::default()
        });
        assert_eq!(tracker.format_cost(42.5), "元42.50");
    }

    // -- format_summary edge cases --
    #[test]
    fn format_summary_no_peak_task() {
        let tracker = CostTracker::new();
        let summary = tracker.summary_all();
        let formatted = tracker.format_summary(&summary);
        assert!(formatted.contains("Cost Summary"));
        assert!(formatted.contains("all-time"));
        assert!(!formatted.contains("Peak:"));
    }

    #[test]
    fn format_summary_with_peak_task() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "peak.zip", 1_073_741_824, now);
        let summary = tracker.summary_for_date("2026-08-11");
        let formatted = tracker.format_summary(&summary);
        assert!(formatted.contains("Peak: peak.zip"));
    }

    #[test]
    fn format_summary_no_budget_section() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            monthly_budget: 0.0,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "f.zip", 100, now);
        let summary = tracker.summary_all();
        let formatted = tracker.format_summary(&summary);
        assert!(!formatted.contains("Budget:"));
    }

    #[test]
    fn format_summary_budget_alert_warning() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 100.0)],
            monthly_budget: 10.0,
            alert_threshold: 0.5,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "f.zip", 1_073_741_824, now);
        let summary = tracker.summary_for_date("2026-08-11");
        assert!(summary.budget_alert);
        let formatted = tracker.format_summary(&summary);
        assert!(formatted.contains("Budget alert"));
    }

    // -- clear edge cases --
    #[test]
    fn clear_empty_tracker() {
        let mut tracker = CostTracker::new();
        tracker.clear();
        assert_eq!(tracker.all_task_records().len(), 0);
        assert!(tracker.daily_usage().is_empty());
    }

    // -- prune_daily_usage edge cases --
    #[test]
    fn prune_daily_usage_empty() {
        let mut tracker = CostTracker::new();
        tracker.prune_daily_usage(30);
        assert!(tracker.daily_usage().is_empty());
    }

    #[test]
    fn prune_daily_usage_keep_zero_days() {
        let mut tracker = CostTracker::new();
        tracker.daily_usage.insert(
            "2099-12-31".to_string(),
            DailyCostUsage {
                date: "2099-12-31".to_string(),
                bytes_downloaded: 100,
                total_cost: 0.01,
                task_count: 1,
            },
        );
        tracker.prune_daily_usage(0);
        // Future date should remain even with 0 days
        assert!(tracker.daily_usage().contains_key("2099-12-31"));
    }

    // -- persistence edge cases --
    #[test]
    fn save_load_config_missing_file() {
        let result = CostTracker::load_config(Path::new("/tmp/nonexistent_cost_config_test.json"));
        assert!(result.is_err());
    }

    #[test]
    fn save_load_state_missing_file() {
        let result = CostTracker::load_state(Path::new("/tmp/nonexistent_cost_state_test.json"));
        assert!(result.is_err());
    }

    #[test]
    fn load_config_corrupted_json() {
        let tmp = std::env::temp_dir().join("cost_config_corrupted_test.json");
        std::fs::write(&tmp, "not valid json{{{").unwrap();
        let result = CostTracker::load_config(&tmp);
        assert!(result.is_err());
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn load_state_corrupted_json() {
        let tmp = std::env::temp_dir().join("cost_state_corrupted_test.json");
        std::fs::write(&tmp, "not valid json{{{").unwrap();
        let result = CostTracker::load_state(&tmp);
        assert!(result.is_err());
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn save_config_creates_file() {
        let tmp = std::env::temp_dir().join("cost_config_create_test.json");
        if tmp.exists() {
            std::fs::remove_file(&tmp).ok();
        }
        let tracker = CostTracker::new();
        tracker.save_config(&tmp).unwrap();
        assert!(tmp.exists());
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn save_state_creates_file() {
        let tmp = std::env::temp_dir().join("cost_state_create_test.json");
        if tmp.exists() {
            std::fs::remove_file(&tmp).ok();
        }
        let tracker = CostTracker::new();
        tracker.save_state(&tmp).unwrap();
        assert!(tmp.exists());
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn save_config_overwrite() {
        let tmp = std::env::temp_dir().join("cost_config_overwrite_test.json");
        let tracker1 = CostTracker::with_config(CostConfig {
            currency: "$".to_string(),
            ..Default::default()
        });
        tracker1.save_config(&tmp).unwrap();
        let tracker2 = CostTracker::with_config(CostConfig {
            currency: "€".to_string(),
            ..Default::default()
        });
        tracker2.save_config(&tmp).unwrap();
        let loaded = CostTracker::load_config(&tmp).unwrap();
        assert_eq!(loaded.currency, "€");
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn save_state_unicode_roundtrip() {
        let tmp = std::env::temp_dir().join("cost_state_unicode_test.json");
        let config = CostConfig {
            enabled: true,
            currency: "元".to_string(),
            rates: vec![CostRate::flat("默认", 5.0)],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "文件.zip", 1_073_741_824, now);
        tracker.save_state(&tmp).unwrap();
        let loaded = CostTracker::load_state(&tmp).unwrap();
        assert_eq!(loaded.config().currency, "元");
        let record = loaded.get_task_cost("t1").unwrap();
        assert_eq!(record.task_name, "文件.zip");
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn save_state_no_tmp_residual() {
        let tmp = std::env::temp_dir().join("cost_state_no_tmp_test.json");
        let tracker = CostTracker::new();
        tracker.save_state(&tmp).unwrap();
        // The file should exist at the target path, not as a tmp file
        assert!(tmp.exists());
        let parent = tmp.parent().unwrap();
        let tmp_files: Vec<_> = std::fs::read_dir(parent)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("cost_state_no_tmp_test")
                    && e.path() != tmp
            })
            .collect();
        assert!(tmp_files.is_empty());
        std::fs::remove_file(&tmp).ok();
    }

    // -- CostTracker Debug --
    #[test]
    fn cost_tracker_debug() {
        let tracker = CostTracker::new();
        let debug = format!("{:?}", tracker);
        assert!(debug.contains("CostTracker"));
        assert!(debug.contains("config"));
    }

    // -- Complex workflows --
    #[test]
    fn complex_workflow_full_lifecycle() {
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![
                CostRate::windowed("Peak", 5.0, 8, 0, 22, 0),
                CostRate::windowed("Off-Peak", 1.0, 22, 0, 8, 0),
            ],
            monthly_budget: 100.0,
            alert_threshold: 0.8,
        };
        let mut tracker = CostTracker::with_config(config);

        // Record at peak time
        let peak = make_time(12, 0);
        tracker.record_task_usage("t1", "big.zip", 1_073_741_824, peak); // 1 GB = $5

        // Record at off-peak time
        let off_peak = make_time(23, 0);
        tracker.record_task_usage("t2", "small.zip", 1_073_741_824, off_peak); // 1 GB = $1

        // Verify costs
        assert!((tracker.get_task_cost("t1").unwrap().cost - 5.0).abs() < 0.001);
        assert!((tracker.get_task_cost("t2").unwrap().cost - 1.0).abs() < 0.001);

        // Summary
        let summary = tracker.summary_all();
        assert_eq!(summary.task_count, 2);
        assert!((summary.total_cost - 6.0).abs() < 0.001);

        // Remove one task
        tracker.remove_task("t1");
        assert_eq!(tracker.all_task_records().len(), 1);

        // Clear
        tracker.clear();
        assert_eq!(tracker.all_task_records().len(), 0);
    }

    #[test]
    fn complex_workflow_many_tasks() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            monthly_budget: 1000.0,
            alert_threshold: 0.5,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        for i in 0..50 {
            tracker.record_task_usage(
                &format!("task-{i}"),
                &format!("file-{i}.zip"),
                107_374_182, // ~100 MB each
                now,
            );
        }

        assert_eq!(tracker.all_task_records().len(), 50);
        let summary = tracker.summary_all();
        assert_eq!(summary.task_count, 50);
        assert!(summary.total_bytes > 0);
    }

    #[test]
    fn complex_workflow_update_then_summarize() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);

        // Initial record
        tracker.record_task_usage("t1", "file.zip", 1_073_741_824, now);
        assert!((tracker.get_task_cost("t1").unwrap().cost - 10.0).abs() < 0.001);

        // Update with more bytes
        tracker.record_task_usage("t1", "file.zip", 2_147_483_648, now);
        assert!((tracker.get_task_cost("t1").unwrap().cost - 20.0).abs() < 0.001);

        // Summary should reflect updated value
        let summary = tracker.summary_for_date("2026-08-11");
        assert!((summary.total_cost - 20.0).abs() < 0.001);
    }

    #[test]
    fn complex_workflow_save_load_continue() {
        let tmp = std::env::temp_dir().join("cost_workflow_test.json");
        let config = CostConfig {
            enabled: true,
            currency: "$".to_string(),
            rates: vec![CostRate::flat("D", 5.0)],
            monthly_budget: 200.0,
            alert_threshold: 0.8,
        };

        // Phase 1: create and save
        let mut tracker1 = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker1.record_task_usage("t1", "a.zip", 1_073_741_824, now);
        tracker1.save_state(&tmp).unwrap();

        // Phase 2: load and continue
        let mut tracker2 = CostTracker::load_state(&tmp).unwrap();
        tracker2.record_task_usage("t2", "b.zip", 2_147_483_648, now);

        assert_eq!(tracker2.all_task_records().len(), 2);
        let summary = tracker2.summary_all();
        assert_eq!(summary.task_count, 2);

        std::fs::remove_file(&tmp).ok();
    }

    // -- avg_cost_per_gb --
    #[test]
    fn avg_cost_per_gb_calculation() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "a.zip", 1_073_741_824, now); // 1 GB = $10
        let summary = tracker.summary_all();
        assert!((summary.avg_cost_per_gb - 10.0).abs() < 0.001);
    }

    #[test]
    fn avg_cost_per_gb_zero_bytes() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "empty.zip", 0, now);
        let summary = tracker.summary_all();
        assert!((summary.avg_cost_per_gb - 0.0).abs() < f64::EPSILON);
    }

    // -- CostRate windowed constructor --
    #[test]
    fn cost_rate_windowed_custom_minutes() {
        let rate = CostRate::windowed("Custom", 1.5, 7, 30, 18, 45);
        assert_eq!(rate.start_time, NaiveTime::from_hms_opt(7, 30, 0).unwrap());
        assert_eq!(rate.end_time, NaiveTime::from_hms_opt(18, 45, 0).unwrap());
        assert!(rate.matches(NaiveTime::from_hms_opt(7, 30, 0).unwrap()));
        assert!(rate.matches(NaiveTime::from_hms_opt(12, 0, 0).unwrap()));
        assert!(!rate.matches(NaiveTime::from_hms_opt(18, 45, 0).unwrap()));
        assert!(!rate.matches(NaiveTime::from_hms_opt(6, 0, 0).unwrap()));
    }

    // -- midnight wrapping rate --
    #[test]
    fn midnight_wrapping_rate_boundary() {
        let rate = CostRate::windowed("Night", 0.25, 22, 0, 6, 0);
        // Just before start
        assert!(!rate.matches(NaiveTime::from_hms_opt(21, 59, 0).unwrap()));
        // At start
        assert!(rate.matches(NaiveTime::from_hms_opt(22, 0, 0).unwrap()));
        // At midnight
        assert!(rate.matches(NaiveTime::from_hms_opt(0, 0, 0).unwrap()));
        // Just before end
        assert!(rate.matches(NaiveTime::from_hms_opt(5, 59, 0).unwrap()));
        // At end (exclusive)
        assert!(!rate.matches(NaiveTime::from_hms_opt(6, 0, 0).unwrap()));
    }

    // -- summary_all with no budget --
    #[test]
    fn summary_all_no_budget() {
        let config = CostConfig {
            enabled: true,
            rates: vec![CostRate::flat("D", 1.0)],
            monthly_budget: 0.0,
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "a.zip", 100, now);
        let summary = tracker.summary_all();
        assert!(!summary.budget_alert);
        assert!((summary.budget_usage_pct - 0.0).abs() < f64::EPSILON);
    }

    // -- format_summary unicode --
    #[test]
    fn format_summary_unicode() {
        let config = CostConfig {
            enabled: true,
            currency: "元".to_string(),
            rates: vec![CostRate::flat("默认", 10.0)],
            ..Default::default()
        };
        let mut tracker = CostTracker::with_config(config);
        let now = make_time(12, 0);
        tracker.record_task_usage("t1", "中文文件.zip", 1_073_741_824, now);
        let summary = tracker.summary_for_date("2026-08-11");
        let formatted = tracker.format_summary(&summary);
        assert!(formatted.contains("中文文件.zip"));
        assert!(formatted.contains("元"));
    }
}
