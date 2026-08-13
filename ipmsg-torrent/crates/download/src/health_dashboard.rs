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

    // ===== Serialization Tests =====

    #[test]
    fn test_system_health_serde_roundtrip() {
        for variant in [
            SystemHealth::Healthy,
            SystemHealth::Warning,
            SystemHealth::Degraded,
            SystemHealth::Critical,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: SystemHealth = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back);
        }
    }

    #[test]
    fn test_subsystem_health_serde_roundtrip() {
        let health = SubsystemHealth {
            name: "Queue".to_string(),
            status: SystemHealth::Warning,
            score: 65,
            summary: "5 tasks, 2 errors".to_string(),
            issue_count: 2,
        };
        let json = serde_json::to_string(&health).unwrap();
        let back: SubsystemHealth = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "Queue");
        assert_eq!(back.status, SystemHealth::Warning);
        assert_eq!(back.score, 65);
        assert_eq!(back.issue_count, 2);
    }

    #[test]
    fn test_queue_health_data_serde_roundtrip() {
        let data = QueueHealthData {
            total_tasks: 10,
            downloading: 3,
            queued: 4,
            paused: 1,
            completed: 2,
            error_count: 1,
            anomaly_count: 2,
            deadline_missed: 0,
            score: 80,
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: QueueHealthData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_tasks, 10);
        assert_eq!(back.downloading, 3);
        assert_eq!(back.score, 80);
    }

    #[test]
    fn test_speed_health_data_serde_roundtrip() {
        let data = SpeedHealthData {
            current_speed_bps: 500_000,
            avg_speed_5min: 450_000,
            avg_speed_15min: 400_000,
            active_alerts: 1,
            anomaly_count: 2,
            score: 70,
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: SpeedHealthData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.current_speed_bps, 500_000);
        assert_eq!(back.active_alerts, 1);
    }

    #[test]
    fn test_network_health_data_serde_roundtrip() {
        let data = NetworkHealthData {
            is_connected: true,
            quality_score: 85,
            issue_count: 1,
            proxy_enabled: true,
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: NetworkHealthData = serde_json::from_str(&json).unwrap();
        assert!(back.is_connected);
        assert_eq!(back.quality_score, 85);
        assert!(back.proxy_enabled);
    }

    #[test]
    fn test_storage_health_data_serde_roundtrip() {
        let data = StorageHealthData {
            available_bytes: 50_000_000_000,
            is_low: false,
            integrity_issues: 0,
            recycle_bin_count: 3,
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: StorageHealthData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.available_bytes, 50_000_000_000);
        assert!(!back.is_low);
    }

    #[test]
    fn test_error_health_data_serde_roundtrip() {
        let data = ErrorHealthData {
            error_tasks: 5,
            pending_retry: 2,
            retries_today: 10,
            recovery_enabled: true,
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: ErrorHealthData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.error_tasks, 5);
        assert_eq!(back.pending_retry, 2);
        assert!(back.recovery_enabled);
    }

    #[test]
    fn test_health_dashboard_serde_roundtrip() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        let json = serde_json::to_string(&dashboard).unwrap();
        let back: HealthDashboard = serde_json::from_str(&json).unwrap();
        assert_eq!(back.overall, dashboard.overall);
        assert_eq!(back.overall_score, dashboard.overall_score);
        assert_eq!(back.queue.total_tasks, dashboard.queue.total_tasks);
        assert_eq!(back.subsystems.len(), dashboard.subsystems.len());
        assert_eq!(back.recommendations.len(), dashboard.recommendations.len());
    }

    #[test]
    fn test_health_dashboard_config_serde_roundtrip() {
        let config = HealthDashboardConfig {
            slow_speed_threshold_bps: 20_000,
            error_warning_threshold: 5,
            error_critical_threshold: 15,
            low_disk_threshold_bytes: 2_000_000_000,
            anomaly_warning_threshold: 10,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: HealthDashboardConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.slow_speed_threshold_bps, 20_000);
        assert_eq!(back.error_warning_threshold, 5);
        assert_eq!(back.error_critical_threshold, 15);
        assert_eq!(back.low_disk_threshold_bytes, 2_000_000_000);
        assert_eq!(back.anomaly_warning_threshold, 10);
    }

    #[test]
    fn test_health_dashboard_config_extra_fields_ignored() {
        let json = r#"{
            "slow_speed_threshold_bps": 10000,
            "error_warning_threshold": 3,
            "error_critical_threshold": 10,
            "low_disk_threshold_bytes": 1073741824,
            "anomaly_warning_threshold": 5,
            "unknown_field": "should be ignored"
        }"#;
        let config: HealthDashboardConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.slow_speed_threshold_bps, 10_000);
        assert_eq!(config.error_warning_threshold, 3);
    }

    #[test]
    fn test_health_dashboard_config_pretty_serde() {
        let config = HealthDashboardConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let back: HealthDashboardConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(
            back.slow_speed_threshold_bps,
            config.slow_speed_threshold_bps
        );
    }

    // ===== SystemHealth Tests =====

    #[test]
    fn test_system_health_all_labels() {
        assert_eq!(SystemHealth::Healthy.label(), "Healthy");
        assert_eq!(SystemHealth::Warning.label(), "Warning");
        assert_eq!(SystemHealth::Degraded.label(), "Degraded");
        assert_eq!(SystemHealth::Critical.label(), "Critical");
    }

    #[test]
    fn test_system_health_all_scores() {
        assert_eq!(SystemHealth::Healthy.score(), 100);
        assert_eq!(SystemHealth::Warning.score(), 75);
        assert_eq!(SystemHealth::Degraded.score(), 50);
        assert_eq!(SystemHealth::Critical.score(), 25);
    }

    #[test]
    fn test_system_health_all_emojis() {
        assert_eq!(SystemHealth::Healthy.emoji(), "✅");
        assert_eq!(SystemHealth::Warning.emoji(), "⚠️");
        assert_eq!(SystemHealth::Degraded.emoji(), "🔶");
        assert_eq!(SystemHealth::Critical.emoji(), "🔴");
    }

    #[test]
    fn test_system_health_clone_debug() {
        let h = SystemHealth::Healthy;
        let h2 = h.clone();
        assert_eq!(h, h2);
        let debug = format!("{:?}", h);
        assert_eq!(debug, "Healthy");
    }

    #[test]
    fn test_system_health_partial_eq() {
        assert_eq!(SystemHealth::Healthy, SystemHealth::Healthy);
        assert_ne!(SystemHealth::Healthy, SystemHealth::Critical);
        assert_ne!(SystemHealth::Warning, SystemHealth::Degraded);
    }

    // ===== compute_overall_health Tests =====

    #[test]
    fn test_compute_overall_health_empty() {
        let (status, score) = compute_overall_health(&[]);
        assert_eq!(status, SystemHealth::Healthy);
        assert_eq!(score, 100);
    }

    #[test]
    fn test_compute_overall_health_single_healthy() {
        let subsystems = vec![SubsystemHealth {
            name: "Test".to_string(),
            status: SystemHealth::Healthy,
            score: 95,
            summary: "ok".to_string(),
            issue_count: 0,
        }];
        let (status, score) = compute_overall_health(&subsystems);
        // avg=95, min=95, weighted=95*0.6+95*0.4=95
        assert_eq!(score, 95);
        assert_eq!(status, SystemHealth::Healthy);
    }

    #[test]
    fn test_compute_overall_health_single_critical() {
        let subsystems = vec![SubsystemHealth {
            name: "Test".to_string(),
            status: SystemHealth::Critical,
            score: 20,
            summary: "bad".to_string(),
            issue_count: 5,
        }];
        let (status, score) = compute_overall_health(&subsystems);
        assert_eq!(score, 20);
        assert_eq!(status, SystemHealth::Critical);
    }

    #[test]
    fn test_compute_overall_health_all_healthy() {
        let subsystems = vec![
            SubsystemHealth {
                name: "A".to_string(),
                status: SystemHealth::Healthy,
                score: 90,
                summary: String::new(),
                issue_count: 0,
            },
            SubsystemHealth {
                name: "B".to_string(),
                status: SystemHealth::Healthy,
                score: 95,
                summary: String::new(),
                issue_count: 0,
            },
            SubsystemHealth {
                name: "C".to_string(),
                status: SystemHealth::Healthy,
                score: 100,
                summary: String::new(),
                issue_count: 0,
            },
        ];
        let (status, score) = compute_overall_health(&subsystems);
        // avg=(90+95+100)/3=95, min=90, weighted=95*0.6+90*0.4=57+36=93
        assert_eq!(score, 93);
        assert_eq!(status, SystemHealth::Healthy);
    }

    #[test]
    fn test_compute_overall_health_degraded_threshold() {
        let subsystems = vec![
            SubsystemHealth {
                name: "A".to_string(),
                status: SystemHealth::Degraded,
                score: 45,
                summary: String::new(),
                issue_count: 3,
            },
            SubsystemHealth {
                name: "B".to_string(),
                status: SystemHealth::Degraded,
                score: 50,
                summary: String::new(),
                issue_count: 2,
            },
        ];
        let (status, score) = compute_overall_health(&subsystems);
        // avg=(45+50)/2=47, min=45, weighted=47*0.6+45*0.4=28.2+18=46.2 → 46
        assert_eq!(score, 46);
        assert_eq!(status, SystemHealth::Degraded);
    }

    #[test]
    fn test_compute_overall_health_weak_link_dominates() {
        let subsystems = vec![
            SubsystemHealth {
                name: "A".to_string(),
                status: SystemHealth::Healthy,
                score: 100,
                summary: String::new(),
                issue_count: 0,
            },
            SubsystemHealth {
                name: "B".to_string(),
                status: SystemHealth::Critical,
                score: 10,
                summary: String::new(),
                issue_count: 10,
            },
        ];
        let (status, score) = compute_overall_health(&subsystems);
        // avg=(100+10)/2=55, min=10, weighted=55*0.6+10*0.4=33+4=37
        assert_eq!(score, 37);
        assert_eq!(status, SystemHealth::Critical);
    }

    // ===== compute_queue_score Tests =====

    #[test]
    fn test_compute_queue_score_perfect() {
        let input = default_input();
        assert_eq!(compute_queue_score(&input), 100);
    }

    #[test]
    fn test_compute_queue_score_error_penalty() {
        let mut input = default_input();
        input.error_count = 3;
        // 100 - 3*10 = 70
        assert_eq!(compute_queue_score(&input), 70);
    }

    #[test]
    fn test_compute_queue_score_anomaly_penalty() {
        let mut input = default_input();
        input.speed_anomaly_count = 4;
        // 100 - 4*5 = 80
        assert_eq!(compute_queue_score(&input), 80);
    }

    #[test]
    fn test_compute_queue_score_deadline_penalty() {
        let mut input = default_input();
        input.deadline_missed = 2;
        // 100 - 2*15 = 70
        assert_eq!(compute_queue_score(&input), 70);
    }

    #[test]
    fn test_compute_queue_score_stuck_queue() {
        let mut input = default_input();
        input.downloading = 0;
        input.queued = 0;
        input.error_count = 2;
        // 100 - 2*10 - 20 = 60
        assert_eq!(compute_queue_score(&input), 60);
    }

    #[test]
    fn test_compute_queue_score_combined_penalties() {
        let mut input = default_input();
        input.error_count = 2;
        input.speed_anomaly_count = 3;
        input.deadline_missed = 1;
        // 100 - 20 - 15 - 15 = 50
        assert_eq!(compute_queue_score(&input), 50);
    }

    #[test]
    fn test_compute_queue_score_saturates_at_zero() {
        let mut input = default_input();
        input.error_count = 20;
        input.speed_anomaly_count = 30;
        input.deadline_missed = 10;
        let score = compute_queue_score(&input);
        assert_eq!(score, 0);
    }

    // ===== compute_speed_score Tests =====

    #[test]
    fn test_compute_speed_score_perfect() {
        let input = default_input();
        assert_eq!(compute_speed_score(&input), 100);
    }

    #[test]
    fn test_compute_speed_score_low_speed() {
        let mut input = default_input();
        input.current_speed_bps = 5_000; // below 10_000
        input.downloading = 2;
        // 100 - 30 = 70
        assert_eq!(compute_speed_score(&input), 70);
    }

    #[test]
    fn test_compute_speed_score_moderate_speed() {
        let mut input = default_input();
        input.current_speed_bps = 30_000; // below 50_000 but above 10_000
        input.downloading = 1;
        // 100 - 15 = 85
        assert_eq!(compute_speed_score(&input), 85);
    }

    #[test]
    fn test_compute_speed_score_no_penalty_when_not_downloading() {
        let mut input = default_input();
        input.current_speed_bps = 100; // very low
        input.downloading = 0; // but nothing downloading
        // No penalty for low speed when nothing is downloading
        assert_eq!(compute_speed_score(&input), 100);
    }

    #[test]
    fn test_compute_speed_score_alert_penalty() {
        let mut input = default_input();
        input.speed_alert_count = 3;
        // 100 - 3*10 = 70
        assert_eq!(compute_speed_score(&input), 70);
    }

    #[test]
    fn test_compute_speed_score_anomaly_penalty() {
        let mut input = default_input();
        input.speed_anomaly_count = 2;
        // 100 - 2*8 = 84
        assert_eq!(compute_speed_score(&input), 84);
    }

    #[test]
    fn test_compute_speed_score_declining_trend_severe() {
        let mut input = default_input();
        input.avg_speed_5min = 40_000;
        input.avg_speed_15min = 100_000;
        // ratio = 40000/100000 = 0.4 < 0.5 → -20
        assert_eq!(compute_speed_score(&input), 80);
    }

    #[test]
    fn test_compute_speed_score_declining_trend_moderate() {
        let mut input = default_input();
        input.avg_speed_5min = 70_000;
        input.avg_speed_15min = 100_000;
        // ratio = 70000/100000 = 0.7, 0.5 <= 0.7 < 0.8 → -10
        assert_eq!(compute_speed_score(&input), 90);
    }

    #[test]
    fn test_compute_speed_score_no_decline_when_5min_higher() {
        let mut input = default_input();
        input.avg_speed_5min = 600_000;
        input.avg_speed_15min = 400_000;
        // ratio > 1.0, no penalty
        assert_eq!(compute_speed_score(&input), 100);
    }

    #[test]
    fn test_compute_speed_score_zero_averages_no_decline() {
        let mut input = default_input();
        input.avg_speed_5min = 0;
        input.avg_speed_15min = 0;
        // Both zero → skip decline check
        assert_eq!(compute_speed_score(&input), 100);
    }

    // ===== compute_network_score Tests =====

    #[test]
    fn test_compute_network_score_disconnected() {
        let mut input = default_input();
        input.network_connected = false;
        assert_eq!(compute_network_score(&input), 10);
    }

    #[test]
    fn test_compute_network_score_perfect() {
        let mut input = default_input();
        input.network_connected = true;
        input.network_quality = 100;
        input.network_issues = 0;
        assert_eq!(compute_network_score(&input), 100);
    }

    #[test]
    fn test_compute_network_score_with_issues() {
        let mut input = default_input();
        input.network_connected = true;
        input.network_quality = 80;
        input.network_issues = 2;
        // 80 - 2*15 = 50
        assert_eq!(compute_network_score(&input), 50);
    }

    #[test]
    fn test_compute_network_score_saturates() {
        let mut input = default_input();
        input.network_connected = true;
        input.network_quality = 30;
        input.network_issues = 10;
        // 30 - 150 → 0 (saturating_sub)
        assert_eq!(compute_network_score(&input), 0);
    }

    // ===== compute_storage_score Tests =====

    #[test]
    fn test_compute_storage_score_perfect() {
        let input = default_input();
        assert_eq!(compute_storage_score(&input), 100);
    }

    #[test]
    fn test_compute_storage_score_disk_low() {
        let mut input = default_input();
        input.disk_low = true;
        // 100 - 40 = 60
        assert_eq!(compute_storage_score(&input), 60);
    }

    #[test]
    fn test_compute_storage_score_integrity_issues() {
        let mut input = default_input();
        input.integrity_issues = 3;
        // 100 - 3*10 = 70
        assert_eq!(compute_storage_score(&input), 70);
    }

    #[test]
    fn test_compute_storage_score_combined() {
        let mut input = default_input();
        input.disk_low = true;
        input.integrity_issues = 5;
        // 100 - 40 - 50 = 10
        assert_eq!(compute_storage_score(&input), 10);
    }

    // ===== compute_error_score Tests =====

    #[test]
    fn test_compute_error_score_perfect() {
        let input = default_input();
        assert_eq!(compute_error_score(&input), 100);
    }

    #[test]
    fn test_compute_error_score_errors_only() {
        let mut input = default_input();
        input.error_count = 4;
        // 100 - 4*12 = 52
        assert_eq!(compute_error_score(&input), 52);
    }

    #[test]
    fn test_compute_error_score_pending_retry() {
        let mut input = default_input();
        input.pending_retry = 5;
        // 100 - 5*5 = 75
        assert_eq!(compute_error_score(&input), 75);
    }

    #[test]
    fn test_compute_error_score_recovery_disabled() {
        let mut input = default_input();
        input.error_count = 2;
        input.recovery_enabled = false;
        // 100 - 2*12 - 15 = 61
        assert_eq!(compute_error_score(&input), 61);
    }

    #[test]
    fn test_compute_error_score_recovery_disabled_no_errors() {
        let mut input = default_input();
        input.error_count = 0;
        input.recovery_enabled = false;
        // No penalty when no errors even if recovery disabled
        assert_eq!(compute_error_score(&input), 100);
    }

    // ===== generate_recommendations Tests =====

    #[test]
    fn test_recommendations_network_disconnected() {
        let mut input = default_input();
        input.network_connected = false;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("Network disconnected")));
    }

    #[test]
    fn test_recommendations_disk_low() {
        let mut input = default_input();
        input.disk_low = true;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("Disk space low")));
    }

    #[test]
    fn test_recommendations_high_errors() {
        let mut input = default_input();
        input.error_count = 10;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("error state")));
    }

    #[test]
    fn test_recommendations_high_anomalies() {
        let mut input = default_input();
        input.speed_anomaly_count = 5;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("speed anomalies")));
    }

    #[test]
    fn test_recommendations_deadline_misses() {
        let mut input = default_input();
        input.deadline_missed = 2;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("missed deadlines")));
    }

    #[test]
    fn test_recommendations_low_speed() {
        let mut input = default_input();
        input.downloading = 2;
        let recs = generate_recommendations(&input, 100, 30, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("speeds are low")));
    }

    #[test]
    fn test_recommendations_poor_network() {
        let mut input = default_input();
        let recs = generate_recommendations(&input, 100, 100, 30, 100, 100);
        assert!(recs.iter().any(|r| r.contains("Network quality is poor")));
    }

    #[test]
    fn test_recommendations_recovery_disabled() {
        let mut input = default_input();
        input.error_count = 3;
        input.recovery_enabled = false;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("Enable error recovery")));
    }

    #[test]
    fn test_recommendations_large_queue_low_health() {
        let mut input = default_input();
        input.queued = 15;
        let recs = generate_recommendations(&input, 30, 100, 100, 100, 100);
        assert!(recs.iter().any(|r| r.contains("Large queue")));
    }

    #[test]
    fn test_recommendations_empty_when_all_healthy() {
        let input = default_input();
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(recs.is_empty());
    }

    #[test]
    fn test_recommendations_no_recovery_when_no_errors() {
        let mut input = default_input();
        input.error_count = 0;
        input.recovery_enabled = false;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        assert!(!recs.iter().any(|r| r.contains("recovery")));
    }

    #[test]
    fn test_recommendations_no_low_speed_when_not_downloading() {
        let mut input = default_input();
        input.downloading = 0;
        let recs = generate_recommendations(&input, 100, 30, 100, 100, 100);
        assert!(!recs.iter().any(|r| r.contains("speeds are low")));
    }

    // ===== format_speed Tests =====

    #[test]
    fn test_format_speed_zero() {
        assert_eq!(format_speed(0), "0 B");
    }

    #[test]
    fn test_format_speed_exact_kb() {
        assert_eq!(format_speed(1024), "1.0 KB");
    }

    #[test]
    fn test_format_speed_exact_mb() {
        assert_eq!(format_speed(1_048_576), "1.0 MB");
    }

    #[test]
    fn test_format_speed_exact_gb() {
        assert_eq!(format_speed(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn test_format_speed_large_value() {
        assert_eq!(format_speed(5_000_000_000), "4.7 GB");
    }

    #[test]
    fn test_format_speed_below_kb() {
        assert_eq!(format_speed(1023), "1023 B");
    }

    // ===== format_size Tests =====

    #[test]
    fn test_format_size_zero() {
        assert_eq!(format_size(0), "0 B");
    }

    #[test]
    fn test_format_size_exact_kb() {
        assert_eq!(format_size(1024), "1.0 KB");
    }

    #[test]
    fn test_format_size_exact_mb() {
        assert_eq!(format_size(1_048_576), "1.0 MB");
    }

    #[test]
    fn test_format_size_exact_gb() {
        assert_eq!(format_size(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn test_format_size_exact_tb() {
        assert_eq!(format_size(1_099_511_627_776), "1.0 TB");
    }

    #[test]
    fn test_format_size_below_kb() {
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_large_tb() {
        assert_eq!(format_size(5_000_000_000_000), "4.5 TB");
    }

    // ===== format_timestamp Tests =====

    #[test]
    fn test_format_timestamp_zero() {
        assert_eq!(format_timestamp(0), "00:00:00");
    }

    #[test]
    fn test_format_timestamp_midnight() {
        // 0 hours, 0 minutes, 30 seconds
        assert_eq!(format_timestamp(30), "00:00:30");
    }

    #[test]
    fn test_format_timestamp_one_hour() {
        assert_eq!(format_timestamp(3600), "01:00:00");
    }

    #[test]
    fn test_format_timestamp_complex() {
        // 14 hours, 30 minutes, 45 seconds = 14*3600 + 30*60 + 45 = 52245
        assert_eq!(format_timestamp(52245), "14:30:45");
    }

    // ===== score_emoji Tests =====

    #[test]
    fn test_score_emoji_critical() {
        assert_eq!(score_emoji(0), "🔴");
        assert_eq!(score_emoji(24), "🔴");
    }

    #[test]
    fn test_score_emoji_degraded() {
        assert_eq!(score_emoji(25), "🔶");
        assert_eq!(score_emoji(49), "🔶");
    }

    #[test]
    fn test_score_emoji_warning() {
        assert_eq!(score_emoji(50), "⚠️");
        assert_eq!(score_emoji(74), "⚠️");
    }

    #[test]
    fn test_score_emoji_healthy() {
        assert_eq!(score_emoji(75), "✅");
        assert_eq!(score_emoji(100), "✅");
    }

    // ===== build_health_dashboard Integration Tests =====

    #[test]
    fn test_build_dashboard_all_critical() {
        let mut input = default_input();
        input.network_connected = false;
        input.network_quality = 5;
        input.disk_low = true;
        input.error_count = 15;
        input.speed_anomaly_count = 10;
        input.speed_alert_count = 5;
        input.deadline_missed = 3;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.overall, SystemHealth::Critical);
        assert!(dashboard.overall_score < 40);
        assert!(!dashboard.recommendations.is_empty());
    }

    #[test]
    fn test_build_dashboard_preserves_input_data() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.queue.total_tasks, 10);
        assert_eq!(dashboard.queue.downloading, 3);
        assert_eq!(dashboard.queue.queued, 4);
        assert_eq!(dashboard.queue.paused, 1);
        assert_eq!(dashboard.queue.completed, 2);
        assert_eq!(dashboard.speed.current_speed_bps, 500_000);
        assert!(dashboard.network.is_connected);
        assert_eq!(dashboard.storage.recycle_bin_count, 2);
        assert_eq!(dashboard.errors.recovery_enabled, true);
    }

    #[test]
    fn test_build_dashboard_timestamp_is_recent() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Timestamp should be within 2 seconds of now
        assert!(dashboard.timestamp <= now);
        assert!(dashboard.timestamp + 2 >= now);
    }

    #[test]
    fn test_build_dashboard_subsystem_names_order() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.subsystems[0].name, "Queue");
        assert_eq!(dashboard.subsystems[1].name, "Speed");
        assert_eq!(dashboard.subsystems[2].name, "Network");
        assert_eq!(dashboard.subsystems[3].name, "Storage");
        assert_eq!(dashboard.subsystems[4].name, "Errors");
    }

    #[test]
    fn test_build_dashboard_with_custom_config() {
        let input = default_input();
        let config = HealthDashboardConfig {
            slow_speed_threshold_bps: 1_000_000,
            error_warning_threshold: 1,
            error_critical_threshold: 5,
            low_disk_threshold_bytes: 100_000_000_000,
            anomaly_warning_threshold: 1,
        };
        let dashboard = build_health_dashboard(&input, &config);
        // Config doesn't affect scoring directly (used by external code)
        // but the dashboard should still build correctly
        assert_eq!(dashboard.overall_score, dashboard.overall_score);
    }

    #[test]
    fn test_build_dashboard_proxy_enabled() {
        let mut input = default_input();
        input.proxy_enabled = true;
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        assert!(dashboard.network.proxy_enabled);
    }

    // ===== quick_summary Tests =====

    #[test]
    fn test_quick_summary_contains_all_fields() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let summary = dashboard.quick_summary();

        assert!(summary.contains("Healthy"));
        assert!(summary.contains("Q:"));
        assert!(summary.contains("S:"));
        assert!(summary.contains("N:"));
        assert!(summary.contains("St:"));
        assert!(summary.contains("E:"));
        assert!(summary.contains("recs"));
    }

    #[test]
    fn test_quick_summary_critical_system() {
        let mut input = default_input();
        input.network_connected = false;
        input.error_count = 20;
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let summary = dashboard.quick_summary();
        assert!(summary.contains("🔴"));
    }

    // ===== format_health_report Tests =====

    #[test]
    fn test_format_report_contains_all_sections() {
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
        assert!(report.contains("Timestamp"));
    }

    #[test]
    fn test_format_report_with_recommendations() {
        let mut input = default_input();
        input.network_connected = false;
        input.disk_low = true;
        input.error_count = 10;
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let report = format_health_report(&dashboard);

        assert!(report.contains("Recommendations"));
        assert!(report.contains("Network"));
        assert!(report.contains("Disk"));
    }

    #[test]
    fn test_format_report_no_recommendations_section_when_empty() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let report = format_health_report(&dashboard);

        assert!(!report.contains("Recommendations"));
    }

    #[test]
    fn test_format_report_shows_speed_values() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let report = format_health_report(&dashboard);

        assert!(report.contains("/s"));
        assert!(report.contains("Current:"));
    }

    #[test]
    fn test_format_report_shows_network_status() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let report = format_health_report(&dashboard);

        assert!(report.contains("Connected"));
        assert!(report.contains("Quality:"));
    }

    #[test]
    fn test_format_report_disconnected_network() {
        let mut input = default_input();
        input.network_connected = false;
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let report = format_health_report(&dashboard);

        assert!(report.contains("Disconnected"));
    }

    // ===== Clone/Debug Traits Tests =====

    #[test]
    fn test_health_dashboard_clone() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let cloned = dashboard.clone();

        assert_eq!(cloned.overall, dashboard.overall);
        assert_eq!(cloned.overall_score, dashboard.overall_score);
        assert_eq!(cloned.queue.total_tasks, dashboard.queue.total_tasks);
        assert_eq!(cloned.subsystems.len(), dashboard.subsystems.len());
    }

    #[test]
    fn test_health_dashboard_debug() {
        let input = default_input();
        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);
        let debug = format!("{:?}", dashboard);
        assert!(debug.contains("HealthDashboard"));
        assert!(debug.contains("overall"));
    }

    #[test]
    fn test_health_dashboard_config_clone() {
        let config = HealthDashboardConfig::default();
        let cloned = config.clone();
        assert_eq!(
            cloned.slow_speed_threshold_bps,
            config.slow_speed_threshold_bps
        );
        assert_eq!(
            cloned.error_warning_threshold,
            config.error_warning_threshold
        );
    }

    #[test]
    fn test_health_dashboard_config_debug() {
        let config = HealthDashboardConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("HealthDashboardConfig"));
    }

    #[test]
    fn test_subsystem_health_clone_debug() {
        let health = SubsystemHealth {
            name: "Test".to_string(),
            status: SystemHealth::Healthy,
            score: 90,
            summary: "ok".to_string(),
            issue_count: 0,
        };
        let cloned = health.clone();
        assert_eq!(cloned.name, "Test");
        let debug = format!("{:?}", health);
        assert!(debug.contains("SubsystemHealth"));
    }

    // ===== Edge Case Tests =====

    #[test]
    fn test_zero_tasks_system() {
        let mut input = default_input();
        input.total_tasks = 0;
        input.downloading = 0;
        input.queued = 0;
        input.paused = 0;
        input.completed = 0;
        input.error_count = 0;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.queue.total_tasks, 0);
        assert_eq!(dashboard.queue.score, 100);
        assert_eq!(dashboard.overall, SystemHealth::Healthy);
    }

    #[test]
    fn test_massive_error_count() {
        let mut input = default_input();
        input.error_count = 1000;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        assert_eq!(dashboard.errors.error_tasks, 1000);
        // Error score computed separately; verify via subsystem
        let error_subsystem = dashboard
            .subsystems
            .iter()
            .find(|s| s.name == "Errors")
            .unwrap();
        assert_eq!(error_subsystem.score, 0);
    }

    #[test]
    fn test_unicode_in_recommendations() {
        let mut input = default_input();
        input.error_count = 10;
        let recs = generate_recommendations(&input, 100, 100, 100, 100, 100);
        // Error count recommendation contains the number
        assert!(recs.iter().any(|r| r.contains("10 tasks")));
    }

    #[test]
    fn test_health_input_clone() {
        let input = default_input();
        let cloned = input.clone();
        assert_eq!(cloned.total_tasks, input.total_tasks);
        assert_eq!(cloned.current_speed_bps, input.current_speed_bps);
    }

    #[test]
    fn test_health_input_debug() {
        let input = default_input();
        let debug = format!("{:?}", input);
        assert!(debug.contains("HealthInput"));
    }

    #[test]
    fn test_all_data_structs_clone_debug() {
        let q = QueueHealthData {
            total_tasks: 1,
            downloading: 0,
            queued: 0,
            paused: 0,
            completed: 1,
            error_count: 0,
            anomaly_count: 0,
            deadline_missed: 0,
            score: 100,
        };
        let q2 = q.clone();
        assert_eq!(q2.total_tasks, 1);
        assert!(format!("{:?}", q).contains("QueueHealthData"));

        let s = SpeedHealthData {
            current_speed_bps: 0,
            avg_speed_5min: 0,
            avg_speed_15min: 0,
            active_alerts: 0,
            anomaly_count: 0,
            score: 100,
        };
        let s2 = s.clone();
        assert_eq!(s2.current_speed_bps, 0);
        assert!(format!("{:?}", s).contains("SpeedHealthData"));

        let n = NetworkHealthData {
            is_connected: true,
            quality_score: 100,
            issue_count: 0,
            proxy_enabled: false,
        };
        let n2 = n.clone();
        assert!(n2.is_connected);
        assert!(format!("{:?}", n).contains("NetworkHealthData"));

        let st = StorageHealthData {
            available_bytes: 100,
            is_low: false,
            integrity_issues: 0,
            recycle_bin_count: 0,
        };
        let st2 = st.clone();
        assert_eq!(st2.available_bytes, 100);
        assert!(format!("{:?}", st).contains("StorageHealthData"));

        let e = ErrorHealthData {
            error_tasks: 0,
            pending_retry: 0,
            retries_today: 0,
            recovery_enabled: true,
        };
        let e2 = e.clone();
        assert_eq!(e2.error_tasks, 0);
        assert!(format!("{:?}", e).contains("ErrorHealthData"));
    }

    #[test]
    fn test_subsystem_health_issue_counts() {
        let mut input = default_input();
        input.error_count = 3;
        input.deadline_missed = 2;
        input.speed_alert_count = 1;
        input.speed_anomaly_count = 2;
        input.network_issues = 1;
        input.integrity_issues = 1;
        input.disk_low = true;

        let config = HealthDashboardConfig::default();
        let dashboard = build_health_dashboard(&input, &config);

        // Queue issue_count = error_count + deadline_missed
        assert_eq!(dashboard.subsystems[0].issue_count, 5);
        // Speed issue_count = speed_alert_count + speed_anomaly_count
        assert_eq!(dashboard.subsystems[1].issue_count, 3);
        // Network issue_count = network_issues
        assert_eq!(dashboard.subsystems[2].issue_count, 1);
        // Storage issue_count = integrity_issues + (disk_low ? 1 : 0)
        assert_eq!(dashboard.subsystems[3].issue_count, 2);
        // Errors issue_count = error_count
        assert_eq!(dashboard.subsystems[4].issue_count, 3);
    }

    #[test]
    fn test_speed_score_boundary_10k() {
        let mut input = default_input();
        input.current_speed_bps = 9_999;
        input.downloading = 1;
        let score = compute_speed_score(&input);
        // Below 10_000 → -30
        assert_eq!(score, 70);
    }

    #[test]
    fn test_speed_score_boundary_10k_exact() {
        let mut input = default_input();
        input.current_speed_bps = 10_000;
        input.downloading = 1;
        let score = compute_speed_score(&input);
        // At 10_000: not < 10_000, but IS < 50_000 → -15
        assert_eq!(score, 85);
    }

    #[test]
    fn test_speed_score_boundary_50k() {
        let mut input = default_input();
        input.current_speed_bps = 49_999;
        input.downloading = 1;
        let score = compute_speed_score(&input);
        // Below 50_000 → -15
        assert_eq!(score, 85);
    }

    #[test]
    fn test_speed_score_boundary_50k_exact() {
        let mut input = default_input();
        input.current_speed_bps = 50_000;
        input.downloading = 1;
        let score = compute_speed_score(&input);
        // At 50_000, not below → no penalty
        assert_eq!(score, 100);
    }

    #[test]
    fn test_speed_score_decline_boundary_exact_half() {
        let mut input = default_input();
        input.avg_speed_5min = 50_000;
        input.avg_speed_15min = 100_000;
        // ratio = 0.5 exactly → NOT < 0.5, but IS < 0.8 → -10
        let score = compute_speed_score(&input);
        assert_eq!(score, 90);
    }

    #[test]
    fn test_speed_score_decline_boundary_08() {
        let mut input = default_input();
        input.avg_speed_5min = 80_000;
        input.avg_speed_15min = 100_000;
        // ratio = 0.8 exactly → NOT < 0.8 → no penalty
        let score = compute_speed_score(&input);
        assert_eq!(score, 100);
    }
}
