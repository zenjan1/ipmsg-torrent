//! Download Health Dashboard - Unified system health overview
//!
//! Aggregates data from multiple monitoring modules to provide a single,
//! comprehensive view of the download system's health status.

use serde::{Deserialize, Serialize};

/// Overall system health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SystemHealth {
    /// All systems nominal
    Healthy,
    /// Minor issues detected, system still functional
    Warning,
    /// Significant issues requiring attention
    Degraded,
    /// Critical issues, system may not be functional
    Critical,
}

impl SystemHealth {
    pub fn emoji(&self) -> &'static str {
        match self {
            SystemHealth::Healthy => "✅",
            SystemHealth::Warning => "⚠️",
            SystemHealth::Degraded => "🔶",
            SystemHealth::Critical => "🔴",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            SystemHealth::Healthy => "Healthy",
            SystemHealth::Warning => "Warning",
            SystemHealth::Degraded => "Degraded",
            SystemHealth::Critical => "Critical",
        }
    }

    pub fn score(&self) -> u8 {
        match self {
            SystemHealth::Healthy => 100,
            SystemHealth::Warning => 75,
            SystemHealth::Degraded => 50,
            SystemHealth::Critical => 25,
        }
    }
}

/// Individual subsystem health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemHealth {
    /// Name of the subsystem
    pub name: String,
    /// Health status
    pub status: SystemHealth,
    /// Score 0-100
    pub score: u8,
    /// Brief description of current state
    pub summary: String,
    /// Number of active issues
    pub issue_count: usize,
}

/// Queue status summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealthData {
    /// Total tasks in queue
    pub total_tasks: usize,
    /// Tasks currently downloading
    pub downloading: usize,
    /// Tasks waiting in queue
    pub queued: usize,
    /// Tasks paused
    pub paused: usize,
    /// Tasks completed
    pub completed: usize,
    /// Tasks in error state
    pub error_count: usize,
    /// Tasks with speed anomalies
    pub anomaly_count: usize,
    /// Tasks with missed deadlines
    pub deadline_missed: usize,
    /// Queue health score (0-100)
    pub score: u8,
}

/// Speed and bandwidth health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedHealthData {
    /// Current download speed in bytes/sec
    pub current_speed_bps: u64,
    /// Average speed over last 5 minutes
    pub avg_speed_5min: u64,
    /// Average speed over last 15 minutes
    pub avg_speed_15min: u64,
    /// Number of active speed alerts
    pub active_alerts: usize,
    /// Number of tasks with speed anomalies
    pub anomaly_count: usize,
    /// Speed health score (0-100)
    pub score: u8,
}

/// Network connectivity health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkHealthData {
    /// Whether network is currently connected
    pub is_connected: bool,
    /// Network quality score (0-100)
    pub quality_score: u8,
    /// Number of connection issues detected
    pub issue_count: usize,
    /// Whether proxy is configured
    pub proxy_enabled: bool,
}

/// Storage health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageHealthData {
    /// Available disk space in bytes
    pub available_bytes: u64,
    /// Whether disk space is low
    pub is_low: bool,
    /// Number of tasks with integrity issues
    pub integrity_issues: usize,
    /// Number of tasks in recycle bin
    pub recycle_bin_count: usize,
}

/// Error and recovery health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHealthData {
    /// Tasks currently in error state
    pub error_tasks: usize,
    /// Tasks pending retry (cooldown)
    pub pending_retry: usize,
    /// Total retries today
    pub retries_today: u32,
    /// Error recovery enabled
    pub recovery_enabled: bool,
}

/// Complete health dashboard data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthDashboard {
    /// Overall system health
    pub overall: SystemHealth,
    /// Overall health score (0-100)
    pub overall_score: u8,
    /// Queue health
    pub queue: QueueHealthData,
    /// Speed health
    pub speed: SpeedHealthData,
    /// Network health
    pub network: NetworkHealthData,
    /// Storage health
    pub storage: StorageHealthData,
    /// Error health
    pub errors: ErrorHealthData,
    /// Subsystem health reports
    pub subsystems: Vec<SubsystemHealth>,
    /// Top recommendations for improving system health
    pub recommendations: Vec<String>,
    /// Timestamp of this snapshot
    pub timestamp: u64,
}

/// Configuration for health dashboard thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthDashboardConfig {
    /// Speed below this (bytes/sec) triggers warning
    pub slow_speed_threshold_bps: u64,
    /// Error count above this triggers warning
    pub error_warning_threshold: usize,
    /// Error count above this triggers critical
    pub error_critical_threshold: usize,
    /// Disk space below this (bytes) triggers warning
    pub low_disk_threshold_bytes: u64,
    /// Anomaly count above this triggers warning
    pub anomaly_warning_threshold: usize,
}

impl Default for HealthDashboardConfig {
    fn default() -> Self {
        Self {
            slow_speed_threshold_bps: 10_000, // 10 KB/s
            error_warning_threshold: 3,
            error_critical_threshold: 10,
            low_disk_threshold_bytes: 1_073_741_824, // 1 GB
            anomaly_warning_threshold: 5,
        }
    }
}

/// Input data for computing the health dashboard
#[derive(Debug, Clone)]
pub struct HealthInput {
    pub total_tasks: usize,
    pub downloading: usize,
    pub queued: usize,
    pub paused: usize,
    pub completed: usize,
    pub error_count: usize,
    pub current_speed_bps: u64,
    pub avg_speed_5min: u64,
    pub avg_speed_15min: u64,
    pub speed_alert_count: usize,
    pub speed_anomaly_count: usize,
    pub network_connected: bool,
    pub network_quality: u8,
    pub network_issues: usize,
    pub proxy_enabled: bool,
    pub disk_available_bytes: u64,
    pub disk_low: bool,
    pub integrity_issues: usize,
    pub recycle_bin_count: usize,
    pub pending_retry: usize,
    pub retries_today: u32,
    pub recovery_enabled: bool,
    pub deadline_missed: usize,
}

/// Compute overall health from individual subsystem scores
fn compute_overall_health(subsystems: &[SubsystemHealth]) -> (SystemHealth, u8) {
    if subsystems.is_empty() {
        return (SystemHealth::Healthy, 100);
    }

    let min_score = subsystems.iter().map(|s| s.score).min().unwrap_or(100);
    let avg_score =
        subsystems.iter().map(|s| s.score as u32).sum::<u32>() / subsystems.len() as u32;

    // Weighted: 60% average, 40% minimum (weakest link matters)
    let weighted = (avg_score as f64 * 0.6) + (min_score as f64 * 0.4);
    let overall_score = weighted.round() as u8;

    let status = if overall_score >= 85 {
        SystemHealth::Healthy
    } else if overall_score >= 65 {
        SystemHealth::Warning
    } else if overall_score >= 40 {
        SystemHealth::Degraded
    } else {
        SystemHealth::Critical
    };

    (status, overall_score)
}

/// Compute queue health score
fn compute_queue_score(input: &HealthInput) -> u8 {
    let mut score = 100u32;

    // Penalize for error tasks
    if input.error_count > 0 {
        score = score.saturating_sub(input.error_count as u32 * 10);
    }

    // Penalize for anomalies
    if input.speed_anomaly_count > 0 {
        score = score.saturating_sub(input.speed_anomaly_count as u32 * 5);
    }

    // Penalize for deadline misses
    if input.deadline_missed > 0 {
        score = score.saturating_sub(input.deadline_missed as u32 * 15);
    }

    // Penalize if queue is empty but tasks exist elsewhere (stuck)
    if input.downloading == 0 && input.queued == 0 && input.error_count > 0 {
        score = score.saturating_sub(20);
    }

    score as u8
}

/// Compute speed health score
fn compute_speed_score(input: &HealthInput) -> u8 {
    let mut score = 100u32;

    // Penalize low speed
    if input.current_speed_bps < 10_000 && input.downloading > 0 {
        score = score.saturating_sub(30);
    } else if input.current_speed_bps < 50_000 && input.downloading > 0 {
        score = score.saturating_sub(15);
    }

    // Penalize speed alerts
    score = score.saturating_sub(input.speed_alert_count as u32 * 10);

    // Penalize anomalies
    score = score.saturating_sub(input.speed_anomaly_count as u32 * 8);

    // Penalize declining trend (15min avg much lower than 5min avg)
    if input.avg_speed_5min > 0 && input.avg_speed_15min > 0 {
        let ratio = input.avg_speed_5min as f64 / input.avg_speed_15min as f64;
        if ratio < 0.5 {
            score = score.saturating_sub(20);
        } else if ratio < 0.8 {
            score = score.saturating_sub(10);
        }
    }

    score as u8
}

/// Compute network health score
fn compute_network_score(input: &HealthInput) -> u8 {
    if !input.network_connected {
        return 10;
    }

    let mut score = input.network_quality;

    // Penalize for issues
    score = score.saturating_sub(input.network_issues as u8 * 15);

    score
}

/// Compute storage health score
fn compute_storage_score(input: &HealthInput) -> u8 {
    let mut score = 100u32;

    if input.disk_low {
        score = score.saturating_sub(40);
    }

    if input.integrity_issues > 0 {
        score = score.saturating_sub(input.integrity_issues as u32 * 10);
    }

    score as u8
}

/// Compute error health score
fn compute_error_score(input: &HealthInput) -> u8 {
    let mut score = 100u32;

    score = score.saturating_sub(input.error_count as u32 * 12);
    score = score.saturating_sub(input.pending_retry as u32 * 5);

    if !input.recovery_enabled && input.error_count > 0 {
        score = score.saturating_sub(15);
    }

    score as u8
}

/// Generate recommendations based on health data
fn generate_recommendations(
    input: &HealthInput,
    queue_score: u8,
    speed_score: u8,
    network_score: u8,
    _storage_score: u8,
    _error_score: u8,
) -> Vec<String> {
    let mut recs = Vec::new();

    if !input.network_connected {
        recs.push("Network disconnected - check connectivity".to_string());
    }

    if input.disk_low {
        recs.push("Disk space low - free up space or change save path".to_string());
    }

    if input.error_count > 5 {
        recs.push(format!(
            "{} tasks in error state - check error recovery settings",
            input.error_count
        ));
    }

    if input.speed_anomaly_count > 3 {
        recs.push(format!(
            "{} tasks with speed anomalies - check network stability",
            input.speed_anomaly_count
        ));
    }

    if input.deadline_missed > 0 {
        recs.push(format!(
            "{} tasks missed deadlines - consider increasing deadlines or removing",
            input.deadline_missed
        ));
    }

    if speed_score < 50 && input.downloading > 0 {
        recs.push(
            "Download speeds are low - consider adding mirrors or checking bandwidth".to_string(),
        );
    }

    if network_score < 50 {
        recs.push(
            "Network quality is poor - consider pausing downloads until network recovers"
                .to_string(),
        );
    }

    if !input.recovery_enabled && input.error_count > 0 {
        recs.push("Enable error recovery for automatic retry on failures".to_string());
    }

    if queue_score < 50 && input.queued > 10 {
        recs.push(
            "Large queue with low health - consider increasing concurrency or removing stuck tasks"
                .to_string(),
        );
    }

    recs
}

/// Build the complete health dashboard from input data
pub fn build_health_dashboard(
    input: &HealthInput,
    _config: &HealthDashboardConfig,
) -> HealthDashboard {
    let queue_score = compute_queue_score(input);
    let speed_score = compute_speed_score(input);
    let network_score = compute_network_score(input);
    let storage_score = compute_storage_score(input);
    let error_score = compute_error_score(input);

    let queue_status = if queue_score >= 85 {
        SystemHealth::Healthy
    } else if queue_score >= 65 {
        SystemHealth::Warning
    } else if queue_score >= 40 {
        SystemHealth::Degraded
    } else {
        SystemHealth::Critical
    };

    let speed_status = if speed_score >= 85 {
        SystemHealth::Healthy
    } else if speed_score >= 65 {
        SystemHealth::Warning
    } else if speed_score >= 40 {
        SystemHealth::Degraded
    } else {
        SystemHealth::Critical
    };

    let network_status = if network_score >= 85 {
        SystemHealth::Healthy
    } else if network_score >= 65 {
        SystemHealth::Warning
    } else if network_score >= 40 {
        SystemHealth::Degraded
    } else {
        SystemHealth::Critical
    };

    let storage_status = if storage_score >= 85 {
        SystemHealth::Healthy
    } else if storage_score >= 65 {
        SystemHealth::Warning
    } else if storage_score >= 40 {
        SystemHealth::Degraded
    } else {
        SystemHealth::Critical
    };

    let error_status = if error_score >= 85 {
        SystemHealth::Healthy
    } else if error_score >= 65 {
        SystemHealth::Warning
    } else if error_score >= 40 {
        SystemHealth::Degraded
    } else {
        SystemHealth::Critical
    };

    let subsystems = vec![
        SubsystemHealth {
            name: "Queue".to_string(),
            status: queue_status.clone(),
            score: queue_score,
            summary: format!(
                "{} tasks ({} downloading, {} queued, {} errors)",
                input.total_tasks, input.downloading, input.queued, input.error_count
            ),
            issue_count: input.error_count + input.deadline_missed,
        },
        SubsystemHealth {
            name: "Speed".to_string(),
            status: speed_status.clone(),
            score: speed_score,
            summary: format!(
                "Current: {}/s, 5min avg: {}/s, {} alerts",
                format_speed(input.current_speed_bps),
                format_speed(input.avg_speed_5min),
                input.speed_alert_count
            ),
            issue_count: input.speed_alert_count + input.speed_anomaly_count,
        },
        SubsystemHealth {
            name: "Network".to_string(),
            status: network_status.clone(),
            score: network_score,
            summary: if input.network_connected {
                format!("Connected, quality: {}%", input.network_quality)
            } else {
                "Disconnected".to_string()
            },
            issue_count: input.network_issues,
        },
        SubsystemHealth {
            name: "Storage".to_string(),
            status: storage_status.clone(),
            score: storage_score,
            summary: format!(
                "Available: {}, {} integrity issues",
                format_size(input.disk_available_bytes),
                input.integrity_issues
            ),
            issue_count: input.integrity_issues + if input.disk_low { 1 } else { 0 },
        },
        SubsystemHealth {
            name: "Errors".to_string(),
            status: error_status.clone(),
            score: error_score,
            summary: format!(
                "{} errors, {} pending retry, recovery {}",
                input.error_count,
                input.pending_retry,
                if input.recovery_enabled {
                    "enabled"
                } else {
                    "disabled"
                }
            ),
            issue_count: input.error_count,
        },
    ];

    let (overall, overall_score) = compute_overall_health(&subsystems);

    let recommendations = generate_recommendations(
        input,
        queue_score,
        speed_score,
        network_score,
        storage_score,
        error_score,
    );

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    HealthDashboard {
        overall,
        overall_score,
        queue: QueueHealthData {
            total_tasks: input.total_tasks,
            downloading: input.downloading,
            queued: input.queued,
            paused: input.paused,
            completed: input.completed,
            error_count: input.error_count,
            anomaly_count: input.speed_anomaly_count,
            deadline_missed: input.deadline_missed,
            score: queue_score,
        },
        speed: SpeedHealthData {
            current_speed_bps: input.current_speed_bps,
            avg_speed_5min: input.avg_speed_5min,
            avg_speed_15min: input.avg_speed_15min,
            active_alerts: input.speed_alert_count,
            anomaly_count: input.speed_anomaly_count,
            score: speed_score,
        },
        network: NetworkHealthData {
            is_connected: input.network_connected,
            quality_score: input.network_quality,
            issue_count: input.network_issues,
            proxy_enabled: input.proxy_enabled,
        },
        storage: StorageHealthData {
            available_bytes: input.disk_available_bytes,
            is_low: input.disk_low,
            integrity_issues: input.integrity_issues,
            recycle_bin_count: input.recycle_bin_count,
        },
        errors: ErrorHealthData {
            error_tasks: input.error_count,
            pending_retry: input.pending_retry,
            retries_today: input.retries_today,
            recovery_enabled: input.recovery_enabled,
        },
        subsystems,
        recommendations,
        timestamp,
    }
}

/// Format bytes per second as human-readable speed
pub fn format_speed(bps: u64) -> String {
    if bps >= 1_073_741_824 {
        format!("{:.1} GB", bps as f64 / 1_073_741_824.0)
    } else if bps >= 1_048_576 {
        format!("{:.1} MB", bps as f64 / 1_048_576.0)
    } else if bps >= 1024 {
        format!("{:.1} KB", bps as f64 / 1024.0)
    } else {
        format!("{} B", bps)
    }
}

/// Format bytes as human-readable size
pub fn format_size(bytes: u64) -> String {
    if bytes >= 1_099_511_627_776 {
        format!("{:.1} TB", bytes as f64 / 1_099_511_627_776.0)
    } else if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Format the health dashboard as a human-readable report
pub fn format_health_report(dashboard: &HealthDashboard) -> String {
    let mut report = String::new();

    report.push_str(&format!(
        "\n{} System Health: {} (Score: {}/100)\n",
        dashboard.overall.emoji(),
        dashboard.overall.label(),
        dashboard.overall_score
    ));
    report.push_str(&format!(
        "Timestamp: {}\n",
        format_timestamp(dashboard.timestamp)
    ));
    report.push_str("═".repeat(60).as_str());

    // Queue
    report.push_str(&format!(
        "\n\n{} Queue (Score: {}/100)\n",
        score_emoji(dashboard.queue.score),
        dashboard.queue.score
    ));
    report.push_str(&format!(
        "  Total: {} | Downloading: {} | Queued: {} | Paused: {}\n",
        dashboard.queue.total_tasks,
        dashboard.queue.downloading,
        dashboard.queue.queued,
        dashboard.queue.paused
    ));
    report.push_str(&format!(
        "  Completed: {} | Errors: {} | Anomalies: {} | Deadline Missed: {}\n",
        dashboard.queue.completed,
        dashboard.queue.error_count,
        dashboard.queue.anomaly_count,
        dashboard.queue.deadline_missed
    ));

    // Speed
    report.push_str(&format!(
        "\n{} Speed (Score: {}/100)\n",
        score_emoji(dashboard.speed.score),
        dashboard.speed.score
    ));
    report.push_str(&format!(
        "  Current: {}/s | 5min: {}/s | 15min: {}/s\n",
        format_speed(dashboard.speed.current_speed_bps),
        format_speed(dashboard.speed.avg_speed_5min),
        format_speed(dashboard.speed.avg_speed_15min)
    ));
    report.push_str(&format!(
        "  Alerts: {} | Anomalies: {}\n",
        dashboard.speed.active_alerts, dashboard.speed.anomaly_count
    ));

    // Network
    report.push_str(&format!(
        "\n{} Network (Score: {}/100)\n",
        score_emoji(dashboard.network.quality_score),
        dashboard.network.quality_score
    ));
    report.push_str(&format!(
        "  Status: {} | Quality: {}% | Issues: {} | Proxy: {}\n",
        if dashboard.network.is_connected {
            "Connected"
        } else {
            "Disconnected"
        },
        dashboard.network.quality_score,
        dashboard.network.issue_count,
        if dashboard.network.proxy_enabled {
            "Enabled"
        } else {
            "Disabled"
        }
    ));

    // Storage
    report.push_str(&format!(
        "\n{} Storage (Score: {}/100)\n",
        score_emoji(dashboard.storage.available_bytes as u8),
        dashboard.storage.available_bytes
    ));
    report.push_str(&format!(
        "  Available: {} | Low: {} | Integrity Issues: {} | Recycle Bin: {}\n",
        format_size(dashboard.storage.available_bytes),
        dashboard.storage.is_low,
        dashboard.storage.integrity_issues,
        dashboard.storage.recycle_bin_count
    ));

    // Errors
    report.push_str(&format!(
        "\n{} Errors (Score: {}/100)\n",
        score_emoji(dashboard.errors.error_tasks as u8),
        dashboard.errors.error_tasks
    ));
    report.push_str(&format!(
        "  Error Tasks: {} | Pending Retry: {} | Retries Today: {} | Recovery: {}\n",
        dashboard.errors.error_tasks,
        dashboard.errors.pending_retry,
        dashboard.errors.retries_today,
        if dashboard.errors.recovery_enabled {
            "Enabled"
        } else {
            "Disabled"
        }
    ));

    // Recommendations
    if !dashboard.recommendations.is_empty() {
        report.push_str("\n💡 Recommendations:\n");
        for (i, rec) in dashboard.recommendations.iter().enumerate() {
            report.push_str(&format!("  {}. {}\n", i + 1, rec));
        }
    }

    report.push('\n');
    report
}

fn format_timestamp(ts: u64) -> String {
    // Simple formatting - seconds since epoch to readable
    let secs = ts % 60;
    let mins = (ts / 60) % 60;
    let hours = (ts / 3600) % 24;
    format!("{:02}:{:02}:{:02}", hours, mins, secs)
}

/// Helper function for score emoji
fn score_emoji(score: u8) -> &'static str {
    match score {
        0..=24 => "🔴",
        25..=49 => "🔶",
        50..=74 => "⚠️",
        _ => "✅",
    }
}

impl HealthDashboard {
    /// Get a summary string for quick display
    pub fn quick_summary(&self) -> String {
        format!(
            "{} {} | Q:{} S:{} N:{} St:{} E:{} | {} recs",
            self.overall.emoji(),
            self.overall.label(),
            self.queue.score,
            self.speed.score,
            self.network.quality_score,
            self.storage.available_bytes,
            self.errors.error_tasks,
            self.recommendations.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_input() -> HealthInput {
        HealthInput {
            total_tasks: 10,
            downloading: 3,
            queued: 4,
            paused: 1,
            completed: 2,
            error_count: 0,
            current_speed_bps: 500_000,
            avg_speed_5min: 450_000,
            avg_speed_15min: 400_000,
            speed_alert_count: 0,
            speed_anomaly_count: 0,
            network_connected: true,
            network_quality: 90,
            network_issues: 0,
            proxy_enabled: false,
            disk_available_bytes: 50_000_000_000,
            disk_low: false,
            integrity_issues: 0,
            recycle_bin_count: 2,
            pending_retry: 0,
            retries_today: 0,
            recovery_enabled: true,
            deadline_missed: 0,
        }
    }

    #[test]
    fn test_healthy_system() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.overall, SystemHealth::Healthy);
        assert!(dashboard.overall_score >= 85);
        assert!(dashboard.queue.score >= 85);
        assert!(dashboard.speed.score >= 85);
        assert!(dashboard.network.quality_score >= 85);
        assert!(dashboard.storage.available_bytes > 0);
        assert!(dashboard.errors.error_tasks == 0);
        assert!(dashboard.recommendations.is_empty());
    }

    #[test]
    fn test_warning_with_errors() {
        let mut input = default_input();
        input.error_count = 6;
        input.speed_alert_count = 2;
        input.speed_anomaly_count = 4;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        // Should have some recommendations due to high error count and anomalies
        assert!(!dashboard.recommendations.is_empty());
        // Error health should reflect the errors
        assert!(dashboard.errors.error_tasks > 0);
    }

    #[test]
    fn test_critical_network() {
        let mut input = default_input();
        input.network_connected = false;
        input.network_quality = 10;
        input.network_issues = 5;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.network.quality_score, 10);
        assert!(
            dashboard
                .recommendations
                .iter()
                .any(|r| r.contains("Network"))
        );
    }

    #[test]
    fn test_low_disk_space() {
        let mut input = default_input();
        input.disk_low = true;
        input.disk_available_bytes = 500_000_000; // 500 MB

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert!(dashboard.storage.is_low);
        assert!(dashboard.storage.available_bytes < 1_000_000_000);
        assert!(dashboard.recommendations.iter().any(|r| r.contains("Disk")));
    }

    #[test]
    fn test_speed_anomalies() {
        let mut input = default_input();
        input.speed_anomaly_count = 5;
        input.speed_alert_count = 3;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert!(dashboard.speed.score < 100);
        assert!(
            dashboard
                .recommendations
                .iter()
                .any(|r| r.contains("anomal"))
        );
    }

    #[test]
    fn test_deadline_misses() {
        let mut input = default_input();
        input.deadline_missed = 3;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert!(dashboard.queue.score < 100);
        assert!(
            dashboard
                .recommendations
                .iter()
                .any(|r| r.contains("deadline"))
        );
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500), "500 B");
        assert_eq!(format_speed(1500), "1.5 KB");
        assert_eq!(format_speed(1_500_000), "1.4 MB");
        assert_eq!(format_speed(1_500_000_000), "1.4 GB");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(2048), "2.0 KB");
        assert_eq!(format_size(5_000_000), "4.8 MB");
        assert_eq!(format_size(5_000_000_000), "4.7 GB");
    }

    #[test]
    fn test_system_health_variants() {
        assert_eq!(SystemHealth::Healthy.emoji(), "✅");
        assert_eq!(SystemHealth::Warning.emoji(), "⚠️");
        assert_eq!(SystemHealth::Degraded.emoji(), "🔶");
        assert_eq!(SystemHealth::Critical.emoji(), "🔴");
        assert_eq!(SystemHealth::Healthy.score(), 100);
        assert_eq!(SystemHealth::Critical.score(), 25);
    }

    #[test]
    fn test_empty_queue() {
        let mut input = default_input();
        input.total_tasks = 0;
        input.downloading = 0;
        input.queued = 0;
        input.completed = 0;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.queue.total_tasks, 0);
        assert_eq!(dashboard.queue.score, 100);
    }

    #[test]
    fn test_quick_summary() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        let summary = dashboard.quick_summary();
        assert!(summary.contains("Healthy"));
        assert!(summary.contains("Q:"));
        assert!(summary.contains("S:"));
    }

    #[test]
    fn test_format_health_report() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        let report = format_health_report(&dashboard);
        assert!(report.contains("System Health"));
        assert!(report.contains("Queue"));
        assert!(report.contains("Speed"));
        assert!(report.contains("Network"));
        assert!(report.contains("Storage"));
        assert!(report.contains("Errors"));
    }

    #[test]
    fn test_overall_health_computation() {
        let subsystems = vec![
            SubsystemHealth {
                name: "A".to_string(),
                status: SystemHealth::Healthy,
                score: 95,
                summary: "ok".to_string(),
                issue_count: 0,
            },
            SubsystemHealth {
                name: "B".to_string(),
                status: SystemHealth::Warning,
                score: 60,
                summary: "warn".to_string(),
                issue_count: 2,
            },
        ];

        let (status, score) = compute_overall_health(&subsystems);
        // avg = (95+60)/2 = 77 (integer div), min = 60, weighted = 77*0.6 + 60*0.4 = 46.2 + 24 = 70.2
        assert_eq!(score, 70);
        assert_eq!(status, SystemHealth::Warning);
    }

    #[test]
    fn test_error_recovery_disabled_warning() {
        let mut input = default_input();
        input.error_count = 2;
        input.recovery_enabled = false;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert!(
            dashboard
                .recommendations
                .iter()
                .any(|r| r.contains("recovery"))
        );
    }

    #[test]
    fn test_config_default() {
        let config = HealthDashboardConfig::default();
        assert_eq!(config.slow_speed_threshold_bps, 10_000);
        assert_eq!(config.error_warning_threshold, 3);
        assert_eq!(config.error_critical_threshold, 10);
        assert_eq!(config.low_disk_threshold_bytes, 1_073_741_824);
        assert_eq!(config.anomaly_warning_threshold, 5);
    }

    #[test]
    fn test_subsystem_count() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.subsystems.len(), 5);
        assert!(dashboard.subsystems.iter().any(|s| s.name == "Queue"));
        assert!(dashboard.subsystems.iter().any(|s| s.name == "Speed"));
        assert!(dashboard.subsystems.iter().any(|s| s.name == "Network"));
        assert!(dashboard.subsystems.iter().any(|s| s.name == "Storage"));
        assert!(dashboard.subsystems.iter().any(|s| s.name == "Errors"));
    }
}
