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
        // Find a safe char boundary at or before max_len - 3
        let cut_point = max_len.saturating_sub(3);
        let mut safe_cut = cut_point;
        while safe_cut > 0 && !name.is_char_boundary(safe_cut) {
            safe_cut -= 1;
        }
        format!("{}...", &name[..safe_cut])
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

    // ========== Phase 240: Comprehensive Test Coverage ==========

    // --- ReportPeriod serde roundtrip ---
    #[test]
    fn test_report_period_serde_roundtrip_all_variants() {
        for period in [
            ReportPeriod::Daily,
            ReportPeriod::Weekly,
            ReportPeriod::Monthly,
            ReportPeriod::Custom,
        ] {
            let json = serde_json::to_string(&period).unwrap();
            let deserialized: ReportPeriod = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, period);
        }
    }

    #[test]
    fn test_report_period_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&ReportPeriod::Daily).unwrap(),
            "\"daily\""
        );
        assert_eq!(
            serde_json::to_string(&ReportPeriod::Weekly).unwrap(),
            "\"weekly\""
        );
        assert_eq!(
            serde_json::to_string(&ReportPeriod::Monthly).unwrap(),
            "\"monthly\""
        );
        assert_eq!(
            serde_json::to_string(&ReportPeriod::Custom).unwrap(),
            "\"custom\""
        );
    }

    #[test]
    fn test_report_period_clone_copy() {
        let p = ReportPeriod::Daily;
        let p2 = p;
        assert_eq!(p, p2);
        let p3 = p.clone();
        assert_eq!(p, p3);
    }

    #[test]
    fn test_report_period_debug() {
        let debug_str = format!("{:?}", ReportPeriod::Daily);
        assert!(debug_str.contains("Daily"));
    }

    #[test]
    fn test_report_period_eq() {
        assert_eq!(ReportPeriod::Daily, ReportPeriod::Daily);
        assert_ne!(ReportPeriod::Daily, ReportPeriod::Weekly);
        assert_ne!(ReportPeriod::Monthly, ReportPeriod::Custom);
    }

    // --- ReportConfig tests ---
    #[test]
    fn test_report_config_default_values() {
        let config = ReportConfig::default();
        assert_eq!(config.period, ReportPeriod::Daily);
        assert!(config.custom_start.is_none());
        assert!(config.custom_end.is_none());
        assert!(config.include_top_downloads);
        assert_eq!(config.top_count, 10);
        assert!(config.include_protocol_breakdown);
        assert!(config.include_hourly_distribution);
    }

    #[test]
    fn test_report_config_serde_roundtrip() {
        let config = ReportConfig {
            period: ReportPeriod::Weekly,
            custom_start: None,
            custom_end: None,
            include_top_downloads: false,
            top_count: 5,
            include_protocol_breakdown: false,
            include_hourly_distribution: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ReportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.period, config.period);
        assert_eq!(deserialized.top_count, config.top_count);
        assert_eq!(
            deserialized.include_top_downloads,
            config.include_top_downloads
        );
    }

    #[test]
    fn test_report_config_serde_extra_fields_ignored() {
        let json = r#"{"period":"daily","custom_start":null,"custom_end":null,"include_top_downloads":true,"top_count":10,"include_protocol_breakdown":true,"include_hourly_distribution":true,"extra_field":"ignored"}"#;
        let config: ReportConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.period, ReportPeriod::Daily);
        assert_eq!(config.top_count, 10);
    }

    #[test]
    fn test_report_config_serde_pretty() {
        let config = ReportConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: ReportConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.period, config.period);
    }

    #[test]
    fn test_report_config_clone() {
        let config = ReportConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.period, config.period);
        assert_eq!(cloned.top_count, config.top_count);
    }

    #[test]
    fn test_report_config_clone_independence() {
        let mut config = ReportConfig::default();
        let cloned = config.clone();
        config.top_count = 999;
        assert_ne!(config.top_count, cloned.top_count);
    }

    #[test]
    fn test_report_config_debug() {
        let config = ReportConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ReportConfig"));
        assert!(debug_str.contains("Daily"));
    }

    #[test]
    fn test_report_config_custom_values() {
        let now = Utc::now();
        let config = ReportConfig {
            period: ReportPeriod::Custom,
            custom_start: Some(now - Duration::days(5)),
            custom_end: Some(now),
            include_top_downloads: false,
            top_count: 3,
            include_protocol_breakdown: false,
            include_hourly_distribution: false,
        };
        assert_eq!(config.period, ReportPeriod::Custom);
        assert!(config.custom_start.is_some());
        assert!(config.custom_end.is_some());
        assert_eq!(config.top_count, 3);
    }

    // --- ReportTaskData tests ---
    #[test]
    fn test_report_task_data_clone() {
        let task = ReportTaskData {
            id: "test-1".to_string(),
            name: "Test File".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now(),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec!["tag1".to_string()],
            group: Some("group1".to_string()),
        };
        let cloned = task.clone();
        assert_eq!(cloned.id, task.id);
        assert_eq!(cloned.name, task.name);
        assert_eq!(cloned.bytes_downloaded, task.bytes_downloaded);
    }

    #[test]
    fn test_report_task_data_debug() {
        let task = ReportTaskData {
            id: "test-1".to_string(),
            name: "Test File".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now(),
            completed_at: None,
            failed_at: None,
            tags: vec![],
            group: None,
        };
        let debug_str = format!("{:?}", task);
        assert!(debug_str.contains("ReportTaskData"));
        assert!(debug_str.contains("test-1"));
    }

    #[test]
    fn test_report_task_data_unicode() {
        let task = ReportTaskData {
            id: "unicode-1".to_string(),
            name: "中文文件名 🎉".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now(),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec!["标签".to_string()],
            group: Some("分组".to_string()),
        };
        assert_eq!(task.name, "中文文件名 🎉");
        assert_eq!(task.tags[0], "标签");
    }

    // --- HourlyDistribution tests ---
    #[test]
    fn test_hourly_distribution_default() {
        let hourly = HourlyDistribution::default();
        assert_eq!(hourly.started_per_hour.iter().sum::<u64>(), 0);
        assert_eq!(hourly.completed_per_hour.iter().sum::<u64>(), 0);
    }

    #[test]
    fn test_hourly_distribution_serde_roundtrip() {
        let mut hourly = HourlyDistribution::default();
        hourly.started_per_hour[10] = 5;
        hourly.completed_per_hour[14] = 3;
        let json = serde_json::to_string(&hourly).unwrap();
        let deserialized: HourlyDistribution = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.started_per_hour[10], 5);
        assert_eq!(deserialized.completed_per_hour[14], 3);
    }

    #[test]
    fn test_hourly_distribution_clone() {
        let mut hourly = HourlyDistribution::default();
        hourly.started_per_hour[0] = 100;
        let cloned = hourly.clone();
        assert_eq!(cloned.started_per_hour[0], 100);
    }

    #[test]
    fn test_hourly_distribution_debug() {
        let hourly = HourlyDistribution::default();
        let debug_str = format!("{:?}", hourly);
        assert!(debug_str.contains("HourlyDistribution"));
    }

    // --- ProtocolSummary tests ---
    #[test]
    fn test_protocol_summary_serde_roundtrip() {
        let summary = ProtocolSummary {
            count: 10,
            completed: 8,
            bytes: 1_000_000,
            avg_speed: 500.0,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: ProtocolSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.count, summary.count);
        assert_eq!(deserialized.completed, summary.completed);
        assert_eq!(deserialized.bytes, summary.bytes);
        assert_eq!(deserialized.avg_speed, summary.avg_speed);
    }

    #[test]
    fn test_protocol_summary_clone_debug() {
        let summary = ProtocolSummary {
            count: 5,
            completed: 3,
            bytes: 500,
            avg_speed: 100.0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.count, summary.count);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("ProtocolSummary"));
    }

    // --- TaskSummary tests ---
    #[test]
    fn test_task_summary_serde_roundtrip() {
        let summary = TaskSummary {
            id: "task-1".to_string(),
            name: "Test Task".to_string(),
            protocol: "HTTP".to_string(),
            bytes: 1_000_000,
            duration_secs: Some(3600),
            state: "Complete".to_string(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: TaskSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, summary.id);
        assert_eq!(deserialized.bytes, summary.bytes);
        assert_eq!(deserialized.duration_secs, summary.duration_secs);
    }

    #[test]
    fn test_task_summary_serde_none_duration() {
        let summary = TaskSummary {
            id: "task-2".to_string(),
            name: "Incomplete".to_string(),
            protocol: "HTTP".to_string(),
            bytes: 500,
            duration_secs: None,
            state: "Downloading".to_string(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("null") || !json.contains("duration_secs"));
        let deserialized: TaskSummary = serde_json::from_str(&json).unwrap();
        assert!(deserialized.duration_secs.is_none());
    }

    #[test]
    fn test_task_summary_clone_debug() {
        let summary = TaskSummary {
            id: "1".to_string(),
            name: "Test".to_string(),
            protocol: "HTTP".to_string(),
            bytes: 100,
            duration_secs: Some(60),
            state: "Complete".to_string(),
        };
        let cloned = summary.clone();
        assert_eq!(cloned.id, summary.id);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("TaskSummary"));
    }

    // --- DownloadReport tests ---
    #[test]
    fn test_download_report_serde_roundtrip() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let json = serde_json::to_string(&report).unwrap();
        let deserialized: DownloadReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_downloads, report.total_downloads);
        assert_eq!(deserialized.completed_downloads, report.completed_downloads);
        assert_eq!(deserialized.failed_downloads, report.failed_downloads);
        assert_eq!(deserialized.total_bytes, report.total_bytes);
        assert_eq!(deserialized.period, report.period);
    }

    #[test]
    fn test_download_report_serde_extra_fields_ignored() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let mut json: serde_json::Value = serde_json::to_value(&report).unwrap();
        json.as_object_mut().unwrap().insert(
            "extra_field".to_string(),
            serde_json::Value::String("ignored".to_string()),
        );
        let deserialized: DownloadReport = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.total_downloads, report.total_downloads);
    }

    #[test]
    fn test_download_report_clone() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let cloned = report.clone();
        assert_eq!(cloned.total_downloads, report.total_downloads);
        assert_eq!(cloned.total_bytes, report.total_bytes);
    }

    #[test]
    fn test_download_report_debug() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let debug_str = format!("{:?}", report);
        assert!(debug_str.contains("DownloadReport"));
    }

    // --- format_bytes boundary tests ---
    #[test]
    fn test_format_bytes_zero() {
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn test_format_bytes_exact_kb_boundary() {
        assert_eq!(format_bytes(1024), "1.00 KB");
    }

    #[test]
    fn test_format_bytes_just_below_kb() {
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn test_format_bytes_exact_mb_boundary() {
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
    }

    #[test]
    fn test_format_bytes_just_below_mb() {
        assert_eq!(format_bytes(1024 * 1024 - 1), "1024.00 KB");
    }

    #[test]
    fn test_format_bytes_exact_gb_boundary() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_bytes_just_below_gb() {
        assert_eq!(format_bytes(1024 * 1024 * 1024 - 1), "1024.00 MB");
    }

    #[test]
    fn test_format_bytes_large_value() {
        assert_eq!(format_bytes(10 * 1024 * 1024 * 1024), "10.00 GB");
    }

    #[test]
    fn test_format_bytes_one_byte() {
        assert_eq!(format_bytes(1), "1 B");
    }

    // --- format_duration boundary tests ---
    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn test_format_duration_exactly_60() {
        assert_eq!(format_duration(60), "1m 0s");
    }

    #[test]
    fn test_format_duration_just_below_60() {
        assert_eq!(format_duration(59), "59s");
    }

    #[test]
    fn test_format_duration_exactly_3600() {
        assert_eq!(format_duration(3600), "1h 0m");
    }

    #[test]
    fn test_format_duration_just_below_3600() {
        assert_eq!(format_duration(3599), "59m 59s");
    }

    #[test]
    fn test_format_duration_large_value() {
        assert_eq!(format_duration(86400), "24h 0m");
    }

    #[test]
    fn test_format_duration_one_second() {
        assert_eq!(format_duration(1), "1s");
    }

    // --- truncate_name boundary tests ---
    #[test]
    fn test_truncate_name_exact_length() {
        assert_eq!(truncate_name("12345", 5), "12345");
    }

    #[test]
    fn test_truncate_name_shorter_than_max() {
        assert_eq!(truncate_name("abc", 10), "abc");
    }

    #[test]
    fn test_truncate_name_one_over_max() {
        assert_eq!(truncate_name("123456", 5), "12...");
    }

    #[test]
    fn test_truncate_name_empty_string() {
        assert_eq!(truncate_name("", 10), "");
    }

    #[test]
    fn test_truncate_name_unicode() {
        // Unicode chars are multi-byte but len() counts bytes
        let name = "中文测试文件名";
        let result = truncate_name(name, 10);
        assert!(result.len() <= 10 || result == name);
    }

    #[test]
    fn test_truncate_name_max_len_3() {
        assert_eq!(truncate_name("abcdef", 3), "...");
    }

    // --- generate_report edge cases ---
    #[test]
    fn test_generate_report_single_task() {
        let tasks = vec![ReportTaskData {
            id: "single".to_string(),
            name: "Single Task".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_downloads, 1);
        assert_eq!(report.completed_downloads, 1);
        assert_eq!(report.failed_downloads, 0);
        assert_eq!(report.success_rate, 100.0);
    }

    #[test]
    fn test_generate_report_all_failed() {
        let tasks = vec![
            ReportTaskData {
                id: "1".to_string(),
                name: "Failed 1".to_string(),
                protocol: "HTTP".to_string(),
                state: "Error".to_string(),
                bytes_downloaded: 100,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: None,
                failed_at: Some(Utc::now()),
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "2".to_string(),
                name: "Failed 2".to_string(),
                protocol: "HTTP".to_string(),
                state: "Error".to_string(),
                bytes_downloaded: 200,
                total_bytes: 2000,
                created_at: Utc::now() - Duration::hours(2),
                completed_at: None,
                failed_at: Some(Utc::now()),
                tags: vec![],
                group: None,
            },
        ];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_downloads, 2);
        assert_eq!(report.completed_downloads, 0);
        assert_eq!(report.failed_downloads, 2);
        assert_eq!(report.success_rate, 0.0);
    }

    #[test]
    fn test_generate_report_all_paused() {
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "Paused".to_string(),
            protocol: "HTTP".to_string(),
            state: "Paused".to_string(),
            bytes_downloaded: 500,
            total_bytes: 1000,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: None,
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_downloads, 1);
        assert_eq!(report.completed_downloads, 0);
        assert_eq!(report.failed_downloads, 0);
    }

    #[test]
    fn test_generate_report_multiple_protocols() {
        let tasks = vec![
            ReportTaskData {
                id: "1".to_string(),
                name: "HTTP Task".to_string(),
                protocol: "HTTP".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 1000,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: Some(Utc::now()),
                failed_at: None,
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "2".to_string(),
                name: "Torrent Task".to_string(),
                protocol: "Torrent".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 2000,
                total_bytes: 2000,
                created_at: Utc::now() - Duration::hours(2),
                completed_at: Some(Utc::now()),
                failed_at: None,
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "3".to_string(),
                name: "FTP Task".to_string(),
                protocol: "FTP".to_string(),
                state: "Downloading".to_string(),
                bytes_downloaded: 500,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(3),
                completed_at: None,
                failed_at: None,
                tags: vec![],
                group: None,
            },
        ];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.by_protocol.len(), 3);
        assert!(report.by_protocol.contains_key("HTTP"));
        assert!(report.by_protocol.contains_key("Torrent"));
        assert!(report.by_protocol.contains_key("FTP"));
    }

    #[test]
    fn test_generate_report_zero_bytes() {
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "Zero bytes".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 0,
            total_bytes: 0,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_bytes, 0);
        assert_eq!(report.avg_speed_bps, 0.0);
    }

    #[test]
    fn test_generate_report_tasks_outside_period_filtered() {
        let tasks = vec![ReportTaskData {
            id: "old".to_string(),
            name: "Old Task".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now() - Duration::days(30),
            completed_at: Some(Utc::now() - Duration::days(29)),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig {
            period: ReportPeriod::Daily,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_downloads, 0);
    }

    #[test]
    fn test_generate_report_custom_period_no_start_uses_default() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            period: ReportPeriod::Custom,
            custom_start: None,
            custom_end: None,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);
        // Should use default (now - 1 day, now)
        assert_eq!(report.period, ReportPeriod::Custom);
    }

    #[test]
    fn test_generate_report_top_by_duration_only_completed() {
        let tasks = vec![
            ReportTaskData {
                id: "1".to_string(),
                name: "Completed".to_string(),
                protocol: "HTTP".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 1000,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(2),
                completed_at: Some(Utc::now()),
                failed_at: None,
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "2".to_string(),
                name: "Incomplete".to_string(),
                protocol: "HTTP".to_string(),
                state: "Downloading".to_string(),
                bytes_downloaded: 500,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: None,
                failed_at: None,
                tags: vec![],
                group: None,
            },
        ];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        // Only completed tasks should appear in top_by_duration
        assert_eq!(report.top_by_duration.len(), 1);
        assert_eq!(report.top_by_duration[0].id, "1");
    }

    #[test]
    fn test_generate_report_top_count_zero() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            top_count: 0,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);
        assert_eq!(report.top_by_size.len(), 0);
        assert_eq!(report.top_by_duration.len(), 0);
    }

    #[test]
    fn test_generate_report_large_bytes() {
        let tasks = vec![ReportTaskData {
            id: "large".to_string(),
            name: "Large File".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: u64::MAX / 2,
            total_bytes: u64::MAX / 2,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_bytes, u64::MAX / 2);
    }

    #[test]
    fn test_generate_report_avg_speed_calculation() {
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "Task".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 3600,
            total_bytes: 3600,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        // avg_speed_bps = total_bytes / 3600.0
        assert_eq!(report.avg_speed_bps, 1.0);
    }

    #[test]
    fn test_generate_report_protocol_avg_speed() {
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "Task".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 7200,
            total_bytes: 7200,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let http = &report.by_protocol["HTTP"];
        assert_eq!(http.avg_speed, 2.0);
    }

    #[test]
    fn test_generate_report_protocol_zero_bytes_no_speed() {
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "Task".to_string(),
            protocol: "HTTP".to_string(),
            state: "Downloading".to_string(),
            bytes_downloaded: 0,
            total_bytes: 1000,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: None,
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let http = &report.by_protocol["HTTP"];
        assert_eq!(http.avg_speed, 0.0);
    }

    // --- format_report_markdown tests ---
    #[test]
    fn test_format_markdown_contains_footer() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("Report generated by IPMsg-Torrent"));
    }

    #[test]
    fn test_format_markdown_contains_period_title() {
        let tasks = sample_tasks();
        let config = ReportConfig {
            period: ReportPeriod::Weekly,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("weekly"));
    }

    #[test]
    fn test_format_markdown_empty_report() {
        let tasks: Vec<ReportTaskData> = vec![];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("# 📊 Download Report"));
        assert!(md.contains("**Total Downloads:** 0"));
    }

    #[test]
    fn test_format_markdown_with_unicode_tasks() {
        let tasks = vec![ReportTaskData {
            id: "unicode-1".to_string(),
            name: "中文文件名 🎉".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("中文文件名 🎉"));
    }

    #[test]
    fn test_format_markdown_hourly_distribution_includes_bars() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("### Downloads Started"));
        assert!(md.contains("### Downloads Completed"));
        assert!(md.contains("█"));
    }

    #[test]
    fn test_format_markdown_protocol_breakdown_table() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("| Protocol | Count |"));
        assert!(md.contains("| HTTP |"));
    }

    #[test]
    fn test_format_markdown_top_downloads_table() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("| # | Name | Protocol |"));
        assert!(md.contains("| 1 |"));
    }

    #[test]
    fn test_format_markdown_all_sections_disabled() {
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
        // Summary section should still be present
        assert!(md.contains("## Summary"));
    }

    #[test]
    fn test_format_markdown_long_name_truncated() {
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "This is a very long filename that should be truncated in the report".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now() - Duration::hours(1),
            completed_at: Some(Utc::now()),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let md = format_report_markdown(&report, &config);
        assert!(md.contains("..."));
    }

    // --- Complex workflow tests ---
    #[test]
    fn test_full_workflow_generate_and_serialize() {
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);

        // Serialize report
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.is_empty());

        // Deserialize and verify
        let deserialized: DownloadReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_downloads, 3);
        assert_eq!(deserialized.period, ReportPeriod::Daily);

        // Generate markdown
        let md = format_report_markdown(&deserialized, &config);
        assert!(md.contains("## Summary"));
    }

    #[test]
    fn test_multiple_reports_independent() {
        let tasks = sample_tasks();
        let config1 = ReportConfig {
            period: ReportPeriod::Daily,
            ..Default::default()
        };
        let config2 = ReportConfig {
            period: ReportPeriod::Weekly,
            ..Default::default()
        };
        let report1 = generate_report(&tasks, &config1);
        let report2 = generate_report(&tasks, &config2);
        assert_eq!(report1.period, ReportPeriod::Daily);
        assert_eq!(report2.period, ReportPeriod::Weekly);
    }

    #[test]
    fn test_report_with_mixed_states() {
        let tasks = vec![
            ReportTaskData {
                id: "1".to_string(),
                name: "Complete".to_string(),
                protocol: "HTTP".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 1000,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: Some(Utc::now()),
                failed_at: None,
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "2".to_string(),
                name: "Error".to_string(),
                protocol: "HTTP".to_string(),
                state: "Error".to_string(),
                bytes_downloaded: 500,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: None,
                failed_at: Some(Utc::now()),
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "3".to_string(),
                name: "Paused".to_string(),
                protocol: "HTTP".to_string(),
                state: "Paused".to_string(),
                bytes_downloaded: 250,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: None,
                failed_at: None,
                tags: vec![],
                group: None,
            },
            ReportTaskData {
                id: "4".to_string(),
                name: "Downloading".to_string(),
                protocol: "HTTP".to_string(),
                state: "Downloading".to_string(),
                bytes_downloaded: 100,
                total_bytes: 1000,
                created_at: Utc::now() - Duration::hours(1),
                completed_at: None,
                failed_at: None,
                tags: vec![],
                group: None,
            },
        ];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_downloads, 4);
        assert_eq!(report.completed_downloads, 1);
        assert_eq!(report.failed_downloads, 1);
        assert_eq!(report.success_rate, 25.0);
    }

    #[test]
    fn test_success_rate_precision() {
        // 1 out of 3 completed = 33.333...%
        let tasks = sample_tasks();
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        let expected = (2.0_f64 / 3.0) * 100.0;
        assert!((report.success_rate - expected).abs() < 0.001);
    }

    #[test]
    fn test_hourly_distribution_all_hours_zero_initially() {
        let tasks: Vec<ReportTaskData> = vec![];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        for hour in 0..24 {
            assert_eq!(report.hourly.started_per_hour[hour], 0);
            assert_eq!(report.hourly.completed_per_hour[hour], 0);
        }
    }

    #[test]
    fn test_report_period_boundaries() {
        let now = Utc::now();
        let tasks = vec![
            // Task at period boundary
            ReportTaskData {
                id: "boundary".to_string(),
                name: "Boundary".to_string(),
                protocol: "HTTP".to_string(),
                state: "Complete".to_string(),
                bytes_downloaded: 1000,
                total_bytes: 1000,
                created_at: now - Duration::days(1) + Duration::minutes(1),
                completed_at: Some(now),
                failed_at: None,
                tags: vec![],
                group: None,
            },
        ];
        let config = ReportConfig {
            period: ReportPeriod::Daily,
            ..Default::default()
        };
        let report = generate_report(&tasks, &config);
        assert_eq!(report.total_downloads, 1);
    }

    #[test]
    fn test_generate_report_duration_negative_clamped() {
        // completed_at before created_at should clamp to 0
        let tasks = vec![ReportTaskData {
            id: "1".to_string(),
            name: "Negative Duration".to_string(),
            protocol: "HTTP".to_string(),
            state: "Complete".to_string(),
            bytes_downloaded: 1000,
            total_bytes: 1000,
            created_at: Utc::now(),
            completed_at: Some(Utc::now() - Duration::hours(1)),
            failed_at: None,
            tags: vec![],
            group: None,
        }];
        let config = ReportConfig::default();
        let report = generate_report(&tasks, &config);
        assert_eq!(report.top_by_duration.len(), 1);
        assert_eq!(report.top_by_duration[0].duration_secs, Some(0));
    }
}
