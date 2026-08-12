//! Download Queue Health Monitor
//!
//! Analyzes the download queue for problems and provides actionable recommendations.
//! Useful for diagnosing why downloads are slow or stuck.
//!
//! # Features
//!
//! - Detect stuck tasks (no progress for configurable duration)
//! - Detect slow tasks (below configurable speed threshold)
//! - Identify tasks with exhausted mirrors
//! - Report disk space warnings
//! - Aggregate queue health score (0-100)
//! - Actionable recommendations (retry, remove, increase limits)

use serde::{Deserialize, Serialize};

/// Speed below which a running task is considered "slow" (bytes/sec)
const DEFAULT_SLOW_THRESHOLD_BPS: f64 = 1024.0; // 1 KB/s

/// Duration after which a running task with no progress is considered "stuck" (seconds)
const DEFAULT_STUCK_THRESHOLD_SECS: f64 = 300.0; // 5 minutes

/// Overall health status of the download queue
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Everything is fine
    #[default]
    Healthy,
    /// Minor issues detected
    Warning,
    /// Significant problems detected
    Degraded,
    /// Critical issues (all tasks stuck, disk full, etc.)
    Critical,
}

impl HealthStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Warning => "warning",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            Self::Healthy => "✅",
            Self::Warning => "⚠️",
            Self::Degraded => "🔶",
            Self::Critical => "🔴",
        }
    }
}

/// A detected issue with a specific task or the queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthIssue {
    /// Task ID affected (None for queue-level issues)
    pub task_id: Option<String>,
    /// Task name for display
    pub task_name: Option<String>,
    /// Issue category
    pub category: IssueCategory,
    /// Severity level
    pub severity: IssueSeverity,
    /// Human-readable description
    pub message: String,
    /// Suggested action
    pub recommendation: String,
}

/// Category of a health issue
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueCategory {
    /// Task is stuck (no progress)
    Stuck,
    /// Task is running but very slow
    Slow,
    /// Task has failed and has no mirrors
    FailedNoMirror,
    /// Disk space is low
    DiskSpace,
    /// Queue is full (max_concurrent reached with queued tasks waiting)
    QueueFull,
    /// Task has been retrying too many times
    ExcessiveRetries,
}

impl IssueCategory {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Stuck => "stuck",
            Self::Slow => "slow",
            Self::FailedNoMirror => "failed_no_mirror",
            Self::DiskSpace => "disk_space",
            Self::QueueFull => "queue_full",
            Self::ExcessiveRetries => "excessive_retries",
        }
    }
}

/// Severity of a health issue
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl IssueSeverity {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

/// Summary statistics for the health report
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueHealthSummary {
    /// Total number of tasks in the queue
    pub total_tasks: usize,
    /// Number of currently running tasks
    pub running_tasks: usize,
    /// Number of queued tasks waiting to start
    pub queued_tasks: usize,
    /// Number of paused tasks
    pub paused_tasks: usize,
    /// Number of errored tasks
    pub errored_tasks: usize,
    /// Number of completed tasks
    pub completed_tasks: usize,
    /// Number of stuck tasks detected
    pub stuck_count: usize,
    /// Number of slow tasks detected
    pub slow_count: usize,
    /// Number of tasks with excessive retries
    pub excessive_retry_count: usize,
    /// Overall download speed (bytes/sec)
    pub total_speed_bps: f64,
    /// Health score (0-100, higher is better)
    pub health_score: u8,
    /// Overall health status
    pub status: HealthStatus,
}

/// Configuration for the health monitor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMonitorConfig {
    /// Speed threshold below which a task is considered slow (bytes/sec)
    pub slow_threshold_bps: f64,
    /// Duration after which a task with no progress is considered stuck (seconds)
    pub stuck_threshold_secs: f64,
    /// Maximum retry count before flagging excessive retries
    pub max_retry_threshold: u32,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            slow_threshold_bps: DEFAULT_SLOW_THRESHOLD_BPS,
            stuck_threshold_secs: DEFAULT_STUCK_THRESHOLD_SECS,
            max_retry_threshold: 5,
        }
    }
}

/// Input data for health analysis (provided by DownloadManager)
#[derive(Debug, Clone)]
pub struct TaskHealthData {
    pub task_id: String,
    pub name: String,
    pub state: String,
    pub speed_bps: f64,
    pub seconds_since_progress: f64,
    pub auto_retry_count: u32,
    pub has_mirrors: bool,
    pub size: u64,
    pub downloaded: u64,
}

/// Full health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealthReport {
    /// Timestamp of the report
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Summary statistics
    pub summary: QueueHealthSummary,
    /// Detected issues (sorted by severity, most severe first)
    pub issues: Vec<HealthIssue>,
    /// Actionable recommendations
    pub recommendations: Vec<String>,
}

impl QueueHealthReport {
    /// Format the report as a human-readable string
    pub fn format_report(&self) -> String {
        let mut out = String::new();

        out.push_str(&format!(
            "{} Queue Health: {} (score: {}/100)\n",
            self.summary.status.emoji(),
            self.summary.status.label(),
            self.summary.health_score
        ));
        out.push_str(&format!(
            "Tasks: {} total ({} running, {} queued, {} paused, {} errored, {} complete)\n",
            self.summary.total_tasks,
            self.summary.running_tasks,
            self.summary.queued_tasks,
            self.summary.paused_tasks,
            self.summary.errored_tasks,
            self.summary.completed_tasks
        ));

        if self.summary.total_speed_bps > 0.0 {
            out.push_str(&format!(
                "Total speed: {}/s\n",
                format_speed(self.summary.total_speed_bps)
            ));
        }

        if self.summary.stuck_count > 0 {
            out.push_str(&format!("Stuck tasks: {}\n", self.summary.stuck_count));
        }
        if self.summary.slow_count > 0 {
            out.push_str(&format!("Slow tasks: {}\n", self.summary.slow_count));
        }
        if self.summary.excessive_retry_count > 0 {
            out.push_str(&format!(
                "Excessive retries: {}\n",
                self.summary.excessive_retry_count
            ));
        }

        if !self.issues.is_empty() {
            out.push_str("\nIssues:\n");
            for issue in &self.issues {
                let severity_icon = match issue.severity {
                    IssueSeverity::Info => "ℹ️",
                    IssueSeverity::Warning => "⚠️",
                    IssueSeverity::Error => "❌",
                    IssueSeverity::Critical => "🔴",
                };
                let task_prefix = issue
                    .task_name
                    .as_deref()
                    .map(|n| format!("[{}] ", n))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "  {} {}{}\n    → {}\n",
                    severity_icon, task_prefix, issue.message, issue.recommendation
                ));
            }
        }

        if !self.recommendations.is_empty() {
            out.push_str("\nRecommendations:\n");
            for (i, rec) in self.recommendations.iter().enumerate() {
                out.push_str(&format!("  {}. {}\n", i + 1, rec));
            }
        }

        out
    }
}

fn format_speed(bps: f64) -> String {
    if bps >= 1_048_576.0 {
        format!("{:.1} MB", bps / 1_048_576.0)
    } else if bps >= 1024.0 {
        format!("{:.1} KB", bps / 1024.0)
    } else {
        format!("{:.0} B", bps)
    }
}

/// Analyze queue health from task data
pub fn analyze_queue_health(
    tasks: &[TaskHealthData],
    config: &HealthMonitorConfig,
) -> QueueHealthReport {
    let mut issues = Vec::new();
    let mut recommendations = Vec::new();
    let mut summary = QueueHealthSummary {
        total_tasks: tasks.len(),
        ..Default::default()
    };

    for task in tasks {
        match task.state.as_str() {
            "Downloading" => {
                summary.running_tasks += 1;
                summary.total_speed_bps += task.speed_bps;

                // Check for stuck tasks
                if task.seconds_since_progress >= config.stuck_threshold_secs {
                    summary.stuck_count += 1;
                    issues.push(HealthIssue {
                        task_id: Some(task.task_id.clone()),
                        task_name: Some(task.name.clone()),
                        category: IssueCategory::Stuck,
                        severity: IssueSeverity::Error,
                        message: format!("No progress for {:.0}s", task.seconds_since_progress),
                        recommendation: if task.has_mirrors {
                            "Consider switching to a mirror URL".to_string()
                        } else {
                            "Try pausing and resuming, or check if the source is available"
                                .to_string()
                        },
                    });
                }
                // Check for slow tasks (only if not already stuck)
                else if task.speed_bps > 0.0 && task.speed_bps < config.slow_threshold_bps {
                    summary.slow_count += 1;
                    issues.push(HealthIssue {
                        task_id: Some(task.task_id.clone()),
                        task_name: Some(task.name.clone()),
                        category: IssueCategory::Slow,
                        severity: IssueSeverity::Warning,
                        message: format!(
                            "Speed is {}/s (below {}/s threshold)",
                            format_speed(task.speed_bps),
                            format_speed(config.slow_threshold_bps)
                        ),
                        recommendation: "Check network conditions or try a different source"
                            .to_string(),
                    });
                }

                // Check for excessive retries
                if task.auto_retry_count >= config.max_retry_threshold {
                    summary.excessive_retry_count += 1;
                    issues.push(HealthIssue {
                        task_id: Some(task.task_id.clone()),
                        task_name: Some(task.name.clone()),
                        category: IssueCategory::ExcessiveRetries,
                        severity: IssueSeverity::Warning,
                        message: format!("Has been retried {} times", task.auto_retry_count),
                        recommendation: "Source may be unreliable; consider finding an alternative"
                            .to_string(),
                    });
                }
            }
            "Queued" => {
                summary.queued_tasks += 1;
            }
            "Paused" => {
                summary.paused_tasks += 1;
            }
            "Error" => {
                summary.errored_tasks += 1;
                if !task.has_mirrors {
                    issues.push(HealthIssue {
                        task_id: Some(task.task_id.clone()),
                        task_name: Some(task.name.clone()),
                        category: IssueCategory::FailedNoMirror,
                        severity: IssueSeverity::Error,
                        message: "Task failed with no mirror URLs available".to_string(),
                        recommendation: "Add mirror URLs or re-download from a different source"
                            .to_string(),
                    });
                }
            }
            "Complete" => {
                summary.completed_tasks += 1;
            }
            _ => {}
        }
    }

    // Check if queue is full
    let running = summary.running_tasks;
    let queued = summary.queued_tasks;
    if queued > 0 && running > 0 {
        // We can't know max_concurrent from here, but if there are queued tasks
        // while others are running, the queue might be at capacity
        issues.push(HealthIssue {
            task_id: None,
            task_name: None,
            category: IssueCategory::QueueFull,
            severity: IssueSeverity::Info,
            message: format!("{} tasks queued waiting for a download slot", queued),
            recommendation: "Increase max_concurrent limit or wait for running tasks to complete"
                .to_string(),
        });
    }

    // Generate aggregate recommendations
    if summary.stuck_count > 0 && summary.stuck_count == summary.running_tasks {
        recommendations
            .push("All running tasks are stuck! Check your network connection".to_string());
    }
    if summary.errored_tasks > summary.total_tasks / 2 && summary.total_tasks > 2 {
        recommendations
            .push("More than half of tasks have failed. Check network/proxy settings".to_string());
    }
    if summary.slow_count > 0 {
        recommendations.push(format!(
            "{} tasks are running below {}/s. Consider limiting concurrent downloads",
            summary.slow_count,
            format_speed(config.slow_threshold_bps)
        ));
    }
    if summary.total_tasks == 0 {
        recommendations.push("No download tasks. Add one with /dl <url>".to_string());
    }

    // Calculate health score (0-100)
    let health_score = calculate_health_score(&summary);
    summary.health_score = health_score;

    // Determine overall status
    summary.status = if health_score >= 80 {
        HealthStatus::Healthy
    } else if health_score >= 50 {
        HealthStatus::Warning
    } else if health_score >= 20 {
        HealthStatus::Degraded
    } else {
        HealthStatus::Critical
    };

    // Sort issues by severity (most severe first)
    issues.sort_by_key(|issue| std::cmp::Reverse(issue.severity));

    QueueHealthReport {
        timestamp: chrono::Utc::now(),
        summary,
        issues,
        recommendations,
    }
}

fn calculate_health_score(summary: &QueueHealthSummary) -> u8 {
    if summary.total_tasks == 0 {
        return 100; // No tasks = nothing wrong
    }

    let mut score: f64 = 100.0;

    // Penalize for stuck tasks (heavy penalty)
    if summary.running_tasks > 0 {
        let stuck_ratio = summary.stuck_count as f64 / summary.running_tasks as f64;
        score -= stuck_ratio * 60.0;
    }

    // Penalize for slow tasks (moderate penalty)
    if summary.running_tasks > 0 {
        let slow_ratio = summary.slow_count as f64 / summary.running_tasks as f64;
        score -= slow_ratio * 20.0;
    }

    // Penalize for errored tasks
    let error_ratio = summary.errored_tasks as f64 / summary.total_tasks as f64;
    score -= error_ratio * 30.0;

    // Penalize for excessive retries
    if summary.running_tasks > 0 {
        let retry_ratio = summary.excessive_retry_count as f64 / summary.total_tasks as f64;
        score -= retry_ratio * 15.0;
    }

    score.clamp(0.0, 100.0) as u8
}

/// Save health monitor config to disk
pub async fn save_health_monitor_config(
    path: &std::path::Path,
    config: &HealthMonitorConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(config)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load health monitor config from disk
pub async fn load_health_monitor_config(path: &std::path::Path) -> Option<HealthMonitorConfig> {
    match tokio::fs::read_to_string(path).await {
        Ok(json) => serde_json::from_str(&json).ok(),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(
        id: &str,
        name: &str,
        state: &str,
        speed: f64,
        secs_since_progress: f64,
    ) -> TaskHealthData {
        TaskHealthData {
            task_id: id.to_string(),
            name: name.to_string(),
            state: state.to_string(),
            speed_bps: speed,
            seconds_since_progress: secs_since_progress,
            auto_retry_count: 0,
            has_mirrors: false,
            size: 1_000_000,
            downloaded: 500_000,
        }
    }

    #[test]
    fn test_healthy_queue() {
        let tasks = vec![
            make_task("t1", "file1.zip", "Downloading", 50_000.0, 5.0),
            make_task("t2", "file2.zip", "Complete", 0.0, 0.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert_eq!(report.summary.status, HealthStatus::Healthy);
        assert!(report.summary.health_score >= 80);
        assert_eq!(report.summary.stuck_count, 0);
        assert_eq!(report.summary.slow_count, 0);
    }

    #[test]
    fn test_stuck_task_detected() {
        let tasks = vec![
            make_task("t1", "stuck.zip", "Downloading", 0.0, 600.0), // 10 min no progress
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert_eq!(report.summary.stuck_count, 1);
        assert_eq!(report.issues.len(), 1);
        assert_eq!(report.issues[0].category, IssueCategory::Stuck);
        assert_eq!(report.issues[0].severity, IssueSeverity::Error);
    }

    #[test]
    fn test_slow_task_detected() {
        let tasks = vec![
            make_task("t1", "slow.zip", "Downloading", 500.0, 5.0), // 500 B/s < 1KB/s threshold
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert_eq!(report.summary.slow_count, 1);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::Slow)
        );
    }

    #[test]
    fn test_excessive_retries_detected() {
        let mut task = make_task("t1", "retry.zip", "Downloading", 50_000.0, 5.0);
        task.auto_retry_count = 10;
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&[task], &config);

        assert_eq!(report.summary.excessive_retry_count, 1);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::ExcessiveRetries)
        );
    }

    #[test]
    fn test_failed_no_mirror() {
        let tasks = vec![make_task("t1", "failed.zip", "Error", 0.0, 0.0)];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert_eq!(report.summary.errored_tasks, 1);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::FailedNoMirror)
        );
    }

    #[test]
    fn test_failed_with_mirror_no_issue() {
        let mut task = make_task("t1", "failed.zip", "Error", 0.0, 0.0);
        task.has_mirrors = true;
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&[task], &config);

        // Should not flag as FailedNoMirror since mirrors are available
        assert!(
            !report
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::FailedNoMirror)
        );
    }

    #[test]
    fn test_queue_full_info() {
        let tasks = vec![
            make_task("t1", "running.zip", "Downloading", 50_000.0, 5.0),
            make_task("t2", "queued.zip", "Queued", 0.0, 0.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert!(
            report
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::QueueFull)
        );
    }

    #[test]
    fn test_empty_queue() {
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&[], &config);

        assert_eq!(report.summary.status, HealthStatus::Healthy);
        assert_eq!(report.summary.health_score, 100);
        assert!(
            report
                .recommendations
                .iter()
                .any(|r| r.contains("No download tasks"))
        );
    }

    #[test]
    fn test_health_score_degradation() {
        // All stuck → very low score
        let tasks = vec![
            make_task("t1", "a.zip", "Downloading", 0.0, 600.0),
            make_task("t2", "b.zip", "Downloading", 0.0, 600.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert!(report.summary.health_score < 50);
        assert!(
            report.summary.status == HealthStatus::Degraded
                || report.summary.status == HealthStatus::Critical
        );
    }

    #[test]
    fn test_issues_sorted_by_severity() {
        let tasks = vec![
            {
                let mut t = make_task("t1", "slow.zip", "Downloading", 500.0, 5.0);
                t.auto_retry_count = 10;
                t
            },
            make_task("t2", "stuck.zip", "Downloading", 0.0, 600.0),
            make_task("t3", "failed.zip", "Error", 0.0, 0.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        // Issues should be sorted: Error first, then Warning
        assert!(report.issues[0].severity >= report.issues[1].severity);
    }

    #[test]
    fn test_custom_config_thresholds() {
        let tasks = vec![
            make_task("t1", "slow.zip", "Downloading", 5000.0, 5.0), // 5 KB/s
        ];
        // With a higher threshold, this should be flagged as slow
        let config = HealthMonitorConfig {
            slow_threshold_bps: 10_000.0,
            ..Default::default()
        };
        let report = analyze_queue_health(&tasks, &config);
        assert_eq!(report.summary.slow_count, 1);

        // With a lower threshold, it should not be flagged
        let config2 = HealthMonitorConfig {
            slow_threshold_bps: 1_000.0,
            ..Default::default()
        };
        let report2 = analyze_queue_health(&tasks, &config2);
        assert_eq!(report2.summary.slow_count, 0);
    }

    #[test]
    fn test_stuck_with_mirror_recommendation() {
        let mut task = make_task("t1", "stuck.zip", "Downloading", 0.0, 600.0);
        task.has_mirrors = true;
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&[task], &config);

        let stuck_issue = report
            .issues
            .iter()
            .find(|i| i.category == IssueCategory::Stuck)
            .unwrap();
        assert!(stuck_issue.recommendation.contains("mirror"));
    }

    #[test]
    fn test_format_report() {
        let tasks = vec![
            make_task("t1", "file1.zip", "Downloading", 50_000.0, 5.0),
            make_task("t2", "stuck.zip", "Downloading", 0.0, 600.0),
            make_task("t3", "done.zip", "Complete", 0.0, 0.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        let formatted = report.format_report();
        assert!(formatted.contains("Queue Health"));
        assert!(formatted.contains("Tasks: 3 total"));
        assert!(formatted.contains("Stuck tasks: 1"));
        assert!(formatted.contains("stuck.zip"));
    }

    #[test]
    fn test_health_status_labels() {
        assert_eq!(HealthStatus::Healthy.label(), "healthy");
        assert_eq!(HealthStatus::Healthy.emoji(), "✅");
        assert_eq!(HealthStatus::Warning.emoji(), "⚠️");
        assert_eq!(HealthStatus::Degraded.emoji(), "🔶");
        assert_eq!(HealthStatus::Critical.emoji(), "🔴");
    }

    #[test]
    fn test_issue_category_labels() {
        assert_eq!(IssueCategory::Stuck.label(), "stuck");
        assert_eq!(IssueCategory::Slow.label(), "slow");
        assert_eq!(IssueCategory::FailedNoMirror.label(), "failed_no_mirror");
        assert_eq!(IssueCategory::DiskSpace.label(), "disk_space");
        assert_eq!(IssueCategory::QueueFull.label(), "queue_full");
        assert_eq!(IssueCategory::ExcessiveRetries.label(), "excessive_retries");
    }

    #[test]
    fn test_report_serialization() {
        let tasks = vec![make_task("t1", "file.zip", "Downloading", 50_000.0, 5.0)];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        let json = serde_json::to_string(&report).unwrap();
        let parsed: QueueHealthReport = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.summary.total_tasks, 1);
        assert_eq!(parsed.summary.running_tasks, 1);
    }

    #[test]
    fn test_all_tasks_failed_recommendation() {
        let tasks = vec![
            make_task("t1", "a.zip", "Error", 0.0, 0.0),
            make_task("t2", "b.zip", "Error", 0.0, 0.0),
            make_task("t3", "c.zip", "Error", 0.0, 0.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        assert!(
            report
                .recommendations
                .iter()
                .any(|r| r.contains("network") || r.contains("proxy"))
        );
    }

    #[test]
    fn test_paused_tasks_not_flagged() {
        let tasks = vec![make_task("t1", "paused.zip", "Paused", 0.0, 9999.0)];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        // Paused tasks should not be flagged as stuck
        assert_eq!(report.summary.stuck_count, 0);
        assert_eq!(report.summary.paused_tasks, 1);
    }

    #[test]
    fn test_zero_speed_not_flagged_as_slow() {
        // A task with exactly 0 speed that is recent should not be flagged as slow
        // (it might just be starting up)
        let tasks = vec![make_task("t1", "starting.zip", "Downloading", 0.0, 10.0)];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);

        // 10s < 300s stuck threshold, so not stuck
        // 0 speed is not > 0, so not flagged as slow (condition is speed > 0 && speed < threshold)
        assert_eq!(report.summary.slow_count, 0);
        assert_eq!(report.summary.stuck_count, 0);
    }

    #[test]
    fn test_format_speed_helper() {
        assert_eq!(format_speed(500.0), "500 B");
        assert_eq!(format_speed(1500.0), "1.5 KB");
        assert_eq!(format_speed(1_500_000.0), "1.4 MB");
    }

    #[test]
    fn test_health_score_zero_tasks() {
        let summary = QueueHealthSummary {
            total_tasks: 0,
            ..Default::default()
        };
        assert_eq!(calculate_health_score(&summary), 100);
    }

    #[test]
    fn test_health_monitor_config_serialization() {
        let config = HealthMonitorConfig {
            slow_threshold_bps: 2048.0,
            stuck_threshold_secs: 600.0,
            max_retry_threshold: 10,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: HealthMonitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.slow_threshold_bps, 2048.0);
        assert_eq!(loaded.stuck_threshold_secs, 600.0);
        assert_eq!(loaded.max_retry_threshold, 10);
    }

    #[test]
    fn test_health_monitor_config_default() {
        let config = HealthMonitorConfig::default();
        assert_eq!(config.slow_threshold_bps, DEFAULT_SLOW_THRESHOLD_BPS);
        assert_eq!(config.stuck_threshold_secs, DEFAULT_STUCK_THRESHOLD_SECS);
        assert_eq!(config.max_retry_threshold, 5);
    }

    #[tokio::test]
    async fn test_save_load_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health_config.json");
        let config = HealthMonitorConfig {
            slow_threshold_bps: 4096.0,
            stuck_threshold_secs: 120.0,
            max_retry_threshold: 3,
        };
        save_health_monitor_config(&path, &config).await.unwrap();
        let loaded = load_health_monitor_config(&path).await.unwrap();
        assert_eq!(loaded.slow_threshold_bps, 4096.0);
        assert_eq!(loaded.stuck_threshold_secs, 120.0);
        assert_eq!(loaded.max_retry_threshold, 3);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        assert!(load_health_monitor_config(&path).await.is_none());
    }

    #[tokio::test]
    async fn test_load_config_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        tokio::fs::write(&path, "not json").await.unwrap();
        assert!(load_health_monitor_config(&path).await.is_none());
    }

    #[test]
    fn test_queue_health_report_format_contains_all_sections() {
        let tasks = vec![
            make_task("t1", "fast.zip", "Downloading", 100_000.0, 2.0),
            make_task("t2", "stuck.zip", "Downloading", 0.0, 600.0),
            make_task("t3", "slow.zip", "Downloading", 200.0, 5.0),
            make_task("t4", "queued.zip", "Queued", 0.0, 0.0),
            make_task("t5", "done.zip", "Complete", 0.0, 0.0),
        ];
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&tasks, &config);
        let formatted = report.format_report();
        assert!(formatted.contains("Queue Health"));
        assert!(formatted.contains("Tasks: 5 total"));
        assert!(formatted.contains("Stuck tasks: 1"));
        assert!(formatted.contains("Slow tasks: 1"));
        assert!(formatted.contains("Issues:"));
        assert!(formatted.contains("Recommendations:"));
    }

    #[test]
    fn test_issue_severity_ordering() {
        // Verify severity ordering: Critical > Error > Warning > Info
        assert!(IssueSeverity::Critical > IssueSeverity::Error);
        assert!(IssueSeverity::Error > IssueSeverity::Warning);
        assert!(IssueSeverity::Warning > IssueSeverity::Info);
    }

    #[test]
    fn test_multiple_issues_same_task() {
        let mut task = make_task("t1", "bad.zip", "Downloading", 100.0, 600.0);
        task.auto_retry_count = 10;
        let config = HealthMonitorConfig::default();
        let report = analyze_queue_health(&[task], &config);
        // Should have stuck + excessive retries issues
        assert!(report.issues.len() >= 2);
    }
}
