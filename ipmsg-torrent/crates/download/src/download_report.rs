//! Download Report Generation
//!
//! Generate markdown reports summarizing download activity over configurable periods.

use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Report period types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportPeriod {
    /// Last 24 hours
    Daily,
    /// Last 7 days
    Weekly,
    /// Last 30 days
    Monthly,
    /// Custom period
    Custom,
}

impl std::fmt::Display for ReportPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReportPeriod::Daily => write!(f, "daily"),
            ReportPeriod::Weekly => write!(f, "weekly"),
            ReportPeriod::Monthly => write!(f, "monthly"),
            ReportPeriod::Custom => write!(f, "custom"),
        }
    }
}

/// Configuration for report generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportConfig {
    /// Report period
    pub period: ReportPeriod,
    /// Custom start time (for Custom period)
    pub custom_start: Option<DateTime<Utc>>,
    /// Custom end time (for Custom period)
    pub custom_end: Option<DateTime<Utc>>,
    /// Include top downloads in report
    pub include_top_downloads: bool,
    /// Number of top downloads to include
    pub top_count: usize,
    /// Include protocol breakdown
    pub include_protocol_breakdown: bool,
    /// Include hourly distribution
    pub include_hourly_distribution: bool,
}

impl Default for ReportConfig {
    fn default() -> Self {
        Self {
            period: ReportPeriod::Daily,
            custom_start: None,
            custom_end: None,
            include_top_downloads: true,
            top_count: 10,
            include_protocol_breakdown: true,
            include_hourly_distribution: true,
        }
    }
}

/// Input data for a single download task
#[derive(Debug, Clone)]
pub struct ReportTaskData {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub state: String,
    pub bytes_downloaded: u64,
    pub total_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub tags: Vec<String>,
    pub group: Option<String>,
}

/// Hourly activity distribution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HourlyDistribution {
    /// Downloads started per hour (0-23)
    pub started_per_hour: [u64; 24],
    /// Downloads completed per hour (0-23)
    pub completed_per_hour: [u64; 24],
}

/// Generated report data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadReport {
    /// Report generation time
    pub generated_at: DateTime<Utc>,
    /// Report period
    pub period: ReportPeriod,
    /// Period start time
    pub period_start: DateTime<Utc>,
    /// Period end time
    pub period_end: DateTime<Utc>,
    /// Total downloads in period
    pub total_downloads: u64,
    /// Completed downloads in period
    pub completed_downloads: u64,
    /// Failed downloads in period
    pub failed_downloads: u64,
    /// Total bytes downloaded in period
    pub total_bytes: u64,
    /// Average speed in period (bytes/sec)
    pub avg_speed_bps: f64,
    /// Peak speed in period (bytes/sec)
    pub peak_speed_bps: f64,
    /// Protocol breakdown
    pub by_protocol: HashMap<String, ProtocolSummary>,
    /// Top downloads by size
    pub top_by_size: Vec<TaskSummary>,
    /// Top downloads by duration
    pub top_by_duration: Vec<TaskSummary>,
    /// Hourly distribution
    pub hourly: HourlyDistribution,
    /// Success rate (0-100)
    pub success_rate: f64,
}

/// Protocol summary in report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolSummary {
    pub count: u64,
    pub completed: u64,
    pub bytes: u64,
    pub avg_speed: f64,
}

/// Task summary for top lists
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub bytes: u64,
    pub duration_secs: Option<u64>,
    pub state: String,
}

/// Generate a download report from task data
pub fn generate_report(tasks: &[ReportTaskData], config: &ReportConfig) -> DownloadReport {
    let now = Utc::now();
    let (period_start, period_end) = match config.period {
        ReportPeriod::Daily => (now - Duration::days(1), now),
        ReportPeriod::Weekly => (now - Duration::days(7), now),
        ReportPeriod::Monthly => (now - Duration::days(30), now),
        ReportPeriod::Custom => (
            config.custom_start.unwrap_or(now - Duration::days(1)),
            config.custom_end.unwrap_or(now),
        ),
    };

    // Filter tasks within period
    let period_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.created_at >= period_start && t.created_at <= period_end)
        .cloned()
        .collect();

    let total_downloads = period_tasks.len() as u64;
    let completed_downloads = period_tasks
        .iter()
        .filter(|t| t.state == "Complete")
        .count() as u64;
    let failed_downloads = period_tasks.iter().filter(|t| t.state == "Error").count() as u64;
    let total_bytes = period_tasks.iter().map(|t| t.bytes_downloaded).sum();

    let success_rate = if total_downloads > 0 {
        (completed_downloads as f64 / total_downloads as f64) * 100.0
    } else {
        0.0
    };

    // Protocol breakdown
    let mut by_protocol: HashMap<String, ProtocolSummary> = HashMap::new();
    for task in &period_tasks {
        let entry = by_protocol
            .entry(task.protocol.clone())
            .or_insert(ProtocolSummary {
                count: 0,
                completed: 0,
                bytes: 0,
                avg_speed: 0.0,
            });
        entry.count += 1;
        if task.state == "Complete" {
            entry.completed += 1;
        }
        entry.bytes += task.bytes_downloaded;
    }

    // Calculate avg speed per protocol
    for summary in by_protocol.values_mut() {
        if summary.bytes > 0 {
            // Estimate: bytes / 3600 (assume 1 hour average)
            summary.avg_speed = summary.bytes as f64 / 3600.0;
        }
    }

    // Top by size
    let mut top_by_size: Vec<TaskSummary> = period_tasks
        .iter()
        .map(|t| TaskSummary {
            id: t.id.clone(),
            name: t.name.clone(),
            protocol: t.protocol.clone(),
            bytes: t.bytes_downloaded,
            duration_secs: t
                .completed_at
                .map(|c| (c - t.created_at).num_seconds().max(0) as u64),
            state: t.state.clone(),
        })
        .collect();
    top_by_size.sort_by_key(|t| std::cmp::Reverse(t.bytes));
    if config.include_top_downloads {
        top_by_size.truncate(config.top_count);
    }

    // Top by duration
    let mut top_by_duration: Vec<TaskSummary> = period_tasks
        .iter()
        .filter_map(|t| {
            let duration = t
                .completed_at
                .map(|c| (c - t.created_at).num_seconds().max(0) as u64);
            duration.map(|d| TaskSummary {
                id: t.id.clone(),
                name: t.name.clone(),
                protocol: t.protocol.clone(),
                bytes: t.bytes_downloaded,
                duration_secs: Some(d),
                state: t.state.clone(),
            })
        })
        .collect();
    top_by_duration.sort_by_key(|t| std::cmp::Reverse(t.duration_secs));
    if config.include_top_downloads {
        top_by_duration.truncate(config.top_count);
    }

    // Hourly distribution
    let mut hourly = HourlyDistribution::default();
    for task in &period_tasks {
        let hour = task.created_at.hour() as usize;
        if hour < 24 {
            hourly.started_per_hour[hour] += 1;
        }
        if let Some(completed) = task.completed_at {
            let hour = completed.hour() as usize;
            if hour < 24 {
                hourly.completed_per_hour[hour] += 1;
            }
        }
    }

    // Overall avg speed
    let avg_speed_bps = if total_bytes > 0 {
        total_bytes as f64 / 3600.0
    } else {
        0.0
    };

    DownloadReport {
        generated_at: now,
        period: config.period,
        period_start,
        period_end,
        total_downloads,
        completed_downloads,
        failed_downloads,
        total_bytes,
        avg_speed_bps,
        peak_speed_bps: 0.0, // Would need speed history
        by_protocol,
        top_by_size,
        top_by_duration,
        hourly,
        success_rate,
    }
}

/// Format report as markdown
pub fn format_report_markdown(report: &DownloadReport, config: &ReportConfig) -> String {
    let mut md = String::new();

    md.push_str(&format!("# 📊 Download Report - {}\n\n", report.period));
    md.push_str(&format!(
        "**Generated:** {}\n",
        report.generated_at.format("%Y-%m-%d %H:%M:%S UTC")
    ));
    md.push_str(&format!(
        "**Period:** {} to {}\n\n",
        report.period_start.format("%Y-%m-%d %H:%M"),
        report.period_end.format("%Y-%m-%d %H:%M")
    ));

    // Summary
    md.push_str("## Summary\n\n");
    md.push_str(&format!(
        "- **Total Downloads:** {}\n",
        report.total_downloads
    ));
    md.push_str(&format!(
        "- **Completed:** {} ({:.1}%)\n",
        report.completed_downloads, report.success_rate
    ));
    md.push_str(&format!("- **Failed:** {}\n", report.failed_downloads));
    md.push_str(&format!(
        "- **Total Data:** {}\n",
        format_bytes(report.total_bytes)
    ));
    md.push_str(&format!(
        "- **Avg Speed:** {}/s\n\n",
        format_bytes(report.avg_speed_bps as u64)
    ));

    // Protocol breakdown
    if config.include_protocol_breakdown && !report.by_protocol.is_empty() {
        md.push_str("## Protocol Breakdown\n\n");
        md.push_str("| Protocol | Count | Completed | Data | Avg Speed |\n");
        md.push_str("|----------|-------|-----------|------|----------|\n");
        for (proto, summary) in &report.by_protocol {
            md.push_str(&format!(
                "| {} | {} | {} | {} | {}/s |\n",
                proto,
                summary.count,
                summary.completed,
                format_bytes(summary.bytes),
                format_bytes(summary.avg_speed as u64)
            ));
        }
        md.push('\n');
    }

    // Top downloads by size
    if config.include_top_downloads && !report.top_by_size.is_empty() {
        md.push_str("## Top Downloads by Size\n\n");
        md.push_str("| # | Name | Protocol | Size | Duration | State |\n");
        md.push_str("|---|------|----------|------|----------|-------|\n");
        for (i, task) in report.top_by_size.iter().enumerate() {
            let duration = task
                .duration_secs
                .map(format_duration)
                .unwrap_or_else(|| "-".to_string());
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                i + 1,
                truncate_name(&task.name, 30),
                task.protocol,
                format_bytes(task.bytes),
                duration,
                task.state
            ));
        }
        md.push('\n');
    }

    // Hourly distribution
    if config.include_hourly_distribution {
        md.push_str("## Hourly Activity\n\n");
        md.push_str("### Downloads Started\n\n");
        md.push_str("```\n");
        for hour in 0..24 {
            let count = report.hourly.started_per_hour[hour];
            let bar = "█".repeat((count as usize).min(40));
            md.push_str(&format!("{:02}:00 {:>3} {}\n", hour, count, bar));
        }
        md.push_str("```\n\n");

        md.push_str("### Downloads Completed\n\n");
        md.push_str("```\n");
        for hour in 0..24 {
            let count = report.hourly.completed_per_hour[hour];
            let bar = "█".repeat((count as usize).min(40));
            md.push_str(&format!("{:02}:00 {:>3} {}\n", hour, count, bar));
        }
        md.push_str("```\n\n");
    }

    md.push_str("---\n");
    md.push_str("*Report generated by IPMsg-Torrent*\n");

    md
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tasks() -> Vec<ReportTaskData> {
        vec![
            ReportTaskData {
                id: "1".to_string(),
                name: "Ubuntu 22.04 ISO".to_string(),
                protocol: "HTTP".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 2_500_000_000,
                total_bytes: 2_500_000_000,
                created_at: Utc::now() - Duration::hours(2),
                completed_at: Some(Utc::now() - Duration::hours(1)),
                failed_at: None,
                tags: vec!["linux".to_string()],
                group: Some("ISO".to_string()),
            },
            ReportTaskData {
                id: "2".to_string(),
                name: "Fedora 38".to_string(),
                protocol: "Torrent".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 1_800_000_000,
                total_bytes: 1_800_000_000,
                created_at: Utc::now() - Duration::hours(5),
                completed_at: Some(Utc::now() - Duration::hours(3)),
                failed_at: None,
                tags: vec!["linux".to_string()],
                group: Some("ISO".to_string()),
            },
            ReportTaskData {
                id: "3".to_string(),
                name: "Failed download".to_string(),
                protocol: "HTTP".to_string(),
                state: "Error".to_string(),
                bytes_downloaded: 500_000_000,
                total_bytes: 1_000_000_000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: None,
                failed_at: Some(Utc::now()),
                tags: vec![],
                group: None,
            },
        ]
    }

    #[test]
    fn test_generate_daily_report() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        assert_eq!(report.total_downloads, 3);
        assert_eq!(report.completed_downloads, 2);
        assert_eq!(report.failed_downloads, 1);
        assert_eq!(report.period, ReportPeriod::Daily);
        assert!(report.success_rate > 60.0 && report.success_rate < 70.0);
    }

    #[test]
    fn test_protocol_breakdown() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        assert!(report.by_protocol.contains_key("HTTP"));
        assert!(report.by_protocol.contains_key("Torrent"));

        let http = &report.by_protocol["HTTP"];
        assert_eq!(http.count, 2);
        assert_eq!(http.completed, 1);

        let torrent = &report.by_protocol["Torrent"];
        assert_eq!(torrent.count, 1);
        assert_eq!(torrent.completed, 1);
    }

    #[test]
    fn test_top_downloads_by_size() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        assert!(!report.top_by_size.is_empty());
        assert_eq!(report.top_by_size[0].id, "1"); // Largest first
    }

    #[test]
    fn test_hourly_distribution() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        let total_started: u64 = report.hourly.started_per_hour.iter().sum();
        assert_eq!(total_started, 3);

        let total_completed: u64 = report.hourly.completed_per_hour.iter().sum();
        assert_eq!(total_completed, 2);
    }

    #[test]
    fn test_format_markdown() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);

        assert!(md.contains("# 📊 Download Report"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("## Protocol Breakdown"));
        assert!(md.contains("## Top Downloads by Size"));
        assert!(md.contains("## Hourly Activity"));
        assert!(md.contains("Ubuntu 22.04 ISO"));
    }

    #[test]
    fn test_empty_tasks() {
        let tasks: Vec<ReportTaskData> = vec![];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        assert_eq!(report.total_downloads, 0);
        assert_eq!(report.completed_downloads, 0);
        assert_eq!(report.failed_downloads, 0);
        assert_eq!(report.success_rate, 0.0);
    }

    #[test]
    fn test_custom_period() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            period: ReportPeriod::Custom,
            custom_start: Some(Utc::now() - Duration::hours(10)),
            custom_end: Some(Utc::now()),
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);

        assert_eq!(report.period, ReportPeriod::Custom);
        assert_eq!(report.total_downloads, 3);
    }

    #[test]
    fn test_weekly_period() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            period: ReportPeriod::Weekly,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);

        assert_eq!(report.period, ReportPeriod::Weekly);
        assert_eq!(report.total_downloads, 3);
    }

    #[test]
    fn test_monthly_period() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            period: ReportPeriod::Monthly,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);

        assert_eq!(report.period, ReportPeriod::Monthly);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1_500_000), "1.43 MB");
        assert_eq!(format_bytes(2_500_000_000), "2.33 GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3661), "1h 1m");
    }

    #[test]
    fn test_truncate_name() {
        assert_eq!(truncate_name("short", 10), "short");
        assert_eq!(truncate_name("this is a very long name", 10), "this is...");
    }

    #[test]
    fn test_top_count_limit() {
        let mut tasks = Vec::new();
        for i in 0..20 {
            tasks.push(ReportTaskData {
                id: i.to_string(),
                name: format!("Task {}", i),
                protocol: "HTTP".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: (i as u64 + 1) * 1_000_000,
                total_bytes: (i as u64 + 1) * 1_000_000,
                created_at: Utc::now() - Duration::hours(i as i64),
                completed_at: Some(Utc::now()),
                failed_at: None,
                tags: vec![],
                group: None,
            });
        }

        let config = ReportConfig {
            top_count: 5,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);

        assert_eq!(report.top_by_size.len(), 5);
        assert_eq!(report.top_by_size[0].id, "19"); // Largest
    }

    #[test]
    fn test_disable_sections() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            include_top_downloads: false,
            include_protocol_breakdown: false,
            include_hourly_distribution: false,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);

        assert!(!md.contains("## Protocol Breakdown"));
        assert!(!md.contains("## Top Downloads by Size"));
        assert!(!md.contains("## Hourly Activity"));
    }

    #[test]
    fn test_report_serialization() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: DownloadReport = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_downloads, report.total_downloads);
        assert_eq!(deserialized.completed_downloads, report.completed_downloads);
    }

    #[test]
    fn test_period_display() {
        assert_eq!(ReportPeriod::Daily.to_string(), "daily");
        assert_eq!(ReportPeriod::Weekly.to_string(), "weekly");
        assert_eq!(ReportPeriod::Monthly.to_string(), "monthly");
        assert_eq!(ReportPeriod::Custom.to_string(), "custom");
    }
}
