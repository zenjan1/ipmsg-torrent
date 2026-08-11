//! Download Diagnostics System (Phase 155)
//!
//! Comprehensive diagnostic checks for download issues with actionable recommendations.
//! Analyzes common problems: slow downloads, stuck tasks, connection failures,
//! disk space issues, DNS failures, and provides step-by-step fix suggestions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Diagnostic check category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiagnosticCategory {
    /// Network connectivity issues
    Network,
    /// DNS resolution failures
    Dns,
    /// Disk space or I/O issues
    Disk,
    /// Server-side issues
    Server,
    /// Configuration problems
    Configuration,
    /// Performance bottlenecks
    Performance,
    /// Proxy-related issues
    Proxy,
    /// Queue management issues
    Queue,
}

impl fmt::Display for DiagnosticCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiagnosticCategory::Network => write!(f, "Network"),
            DiagnosticCategory::Dns => write!(f, "DNS"),
            DiagnosticCategory::Disk => write!(f, "Disk"),
            DiagnosticCategory::Server => write!(f, "Server"),
            DiagnosticCategory::Configuration => write!(f, "Configuration"),
            DiagnosticCategory::Performance => write!(f, "Performance"),
            DiagnosticCategory::Proxy => write!(f, "Proxy"),
            DiagnosticCategory::Queue => write!(f, "Queue"),
        }
    }
}

/// Severity level of a diagnostic finding
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    /// Informational, no action needed
    Info,
    /// Warning, may cause issues
    Warning,
    /// Error, likely causing problems
    Error,
    /// Critical, blocking downloads
    Critical,
}

impl fmt::Display for DiagnosticSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiagnosticSeverity::Info => write!(f, "INFO"),
            DiagnosticSeverity::Warning => write!(f, "WARNING"),
            DiagnosticSeverity::Error => write!(f, "ERROR"),
            DiagnosticSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// A single diagnostic finding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticFinding {
    /// Category of the issue
    pub category: DiagnosticCategory,
    /// Severity level
    pub severity: DiagnosticSeverity,
    /// Human-readable title
    pub title: String,
    /// Detailed description of the issue
    pub description: String,
    /// Actionable recommendations to fix the issue
    pub recommendations: Vec<String>,
    /// Related task IDs (if applicable)
    pub related_task_ids: Vec<String>,
}

/// Configuration for the diagnostics system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsConfig {
    /// Enable diagnostics system
    pub enabled: bool,
    /// Minimum speed (bytes/sec) to consider "slow"
    pub slow_download_threshold_bps: u64,
    /// Maximum time (seconds) a task can be stuck before flagging
    pub stuck_task_threshold_secs: u64,
    /// Minimum disk space (bytes) before warning
    pub min_disk_space_bytes: u64,
    /// Maximum retry count before flagging
    pub max_retry_threshold: u32,
    /// Maximum consecutive failures before critical
    pub max_consecutive_failures: u32,
    /// Enable network connectivity checks
    pub check_network: bool,
    /// Enable disk space checks
    pub check_disk: bool,
    /// Enable performance analysis
    pub check_performance: bool,
    /// Enable queue analysis
    pub check_queue: bool,
    /// Maximum findings per category
    pub max_findings_per_category: usize,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            slow_download_threshold_bps: 10_000, // 10 KB/s
            stuck_task_threshold_secs: 1800,     // 30 minutes
            min_disk_space_bytes: 1_073_741_824, // 1 GB
            max_retry_threshold: 5,
            max_consecutive_failures: 3,
            check_network: true,
            check_disk: true,
            check_performance: true,
            check_queue: true,
            max_findings_per_category: 10,
        }
    }
}

/// Input data for diagnostics analysis
#[derive(Debug, Clone, Default)]
pub struct DiagnosticsInput {
    /// Current download speed (bytes/sec)
    pub current_speed_bps: u64,
    /// Average download speed (bytes/sec)
    pub avg_speed_bps: u64,
    /// Available disk space (bytes)
    pub available_disk_bytes: u64,
    /// Total disk space (bytes)
    pub total_disk_bytes: u64,
    /// Network connectivity status (true = connected)
    pub network_connected: bool,
    /// DNS resolution working
    pub dns_working: bool,
    /// Proxy configured
    pub proxy_configured: bool,
    /// Proxy reachable (if configured)
    pub proxy_reachable: Option<bool>,
    /// Active download count
    pub active_downloads: usize,
    /// Queued download count
    pub queued_downloads: usize,
    /// Failed download count
    pub failed_downloads: usize,
    /// Stalled download count
    pub stalled_downloads: usize,
    /// Maximum concurrent downloads allowed
    pub max_concurrent: usize,
    /// Tasks with their diagnostic data
    pub task_diagnostics: Vec<TaskDiagnosticData>,
}

/// Per-task diagnostic data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDiagnosticData {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Current state
    pub state: String,
    /// Download speed (bytes/sec)
    pub speed_bps: u64,
    /// Progress percentage (0-100)
    pub progress_percent: f64,
    /// Time since last progress update (seconds)
    pub secs_since_last_progress: u64,
    /// Number of retries
    pub retry_count: u32,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Last error message (if any)
    pub last_error: Option<String>,
    /// Task age in seconds
    pub age_secs: u64,
    /// Expected total size (bytes)
    pub total_size: u64,
    /// Downloaded bytes
    pub downloaded_bytes: u64,
}

/// Summary of diagnostics results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsSummary {
    /// Total findings count
    pub total_findings: usize,
    /// Findings by severity
    pub findings_by_severity: HashMap<String, usize>,
    /// Findings by category
    pub findings_by_category: HashMap<String, usize>,
    /// Critical findings count
    pub critical_count: usize,
    /// Error findings count
    pub error_count: usize,
    /// Warning findings count
    pub warning_count: usize,
    /// Info findings count
    pub info_count: usize,
    /// Overall health score (0-100)
    pub health_score: u32,
    /// Top recommendations
    pub top_recommendations: Vec<String>,
}

/// The diagnostics engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadDiagnostics {
    config: DiagnosticsConfig,
}

impl Default for DownloadDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadDiagnostics {
    /// Create a new diagnostics engine with default config
    pub fn new() -> Self {
        Self {
            config: DiagnosticsConfig::default(),
        }
    }

    /// Create with custom config
    pub fn with_config(config: DiagnosticsConfig) -> Self {
        Self { config }
    }

    /// Get current config
    pub fn get_config(&self) -> &DiagnosticsConfig {
        &self.config
    }

    /// Set config
    pub fn set_config(&mut self, config: DiagnosticsConfig) {
        self.config = config;
    }

    /// Run all diagnostic checks and return findings
    pub fn analyze(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut findings = Vec::new();

        if self.config.check_network {
            findings.extend(self.check_network_issues(input));
        }

        if self.config.check_disk {
            findings.extend(self.check_disk_issues(input));
        }

        if self.config.check_performance {
            findings.extend(self.check_performance_issues(input));
        }

        if self.config.check_queue {
            findings.extend(self.check_queue_issues(input));
        }

        findings.extend(self.check_proxy_issues(input));
        findings.extend(self.check_server_issues(input));
        findings.extend(self.check_config_issues(input));

        // Sort by severity (critical first)
        findings.sort_by(|a, b| b.severity.cmp(&a.severity));

        // Limit findings per category
        let mut limited = Vec::new();
        let mut category_counts: HashMap<DiagnosticCategory, usize> = HashMap::new();
        for finding in findings {
            let count = category_counts.entry(finding.category).or_insert(0);
            if *count < self.config.max_findings_per_category {
                limited.push(finding);
                *count += 1;
            }
        }

        limited
    }

    /// Generate a summary from findings
    pub fn summarize(&self, findings: &[DiagnosticFinding]) -> DiagnosticsSummary {
        let mut findings_by_severity = HashMap::new();
        let mut findings_by_category = HashMap::new();
        let mut critical_count = 0;
        let mut error_count = 0;
        let mut warning_count = 0;
        let mut info_count = 0;
        let mut all_recommendations = Vec::new();

        for finding in findings {
            *findings_by_severity
                .entry(format!("{}", finding.severity))
                .or_insert(0) += 1;
            *findings_by_category
                .entry(format!("{}", finding.category))
                .or_insert(0) += 1;

            match finding.severity {
                DiagnosticSeverity::Critical => critical_count += 1,
                DiagnosticSeverity::Error => error_count += 1,
                DiagnosticSeverity::Warning => warning_count += 1,
                DiagnosticSeverity::Info => info_count += 1,
            }

            for rec in &finding.recommendations {
                if !all_recommendations.contains(rec) {
                    all_recommendations.push(rec.clone());
                }
            }
        }

        // Calculate health score (100 = perfect, deduct for issues)
        let health_score = self.calculate_health_score(findings);

        // Top recommendations: prioritize critical/error findings
        let top_recommendations: Vec<String> = findings
            .iter()
            .filter(|f| f.severity >= DiagnosticSeverity::Error)
            .flat_map(|f| f.recommendations.iter().cloned())
            .collect::<Vec<_>>()
            .into_iter()
            .fold(Vec::new(), |mut acc, rec| {
                if !acc.contains(&rec) {
                    acc.push(rec);
                }
                acc
            })
            .into_iter()
            .take(5)
            .collect();

        DiagnosticsSummary {
            total_findings: findings.len(),
            findings_by_severity,
            findings_by_category,
            critical_count,
            error_count,
            warning_count,
            info_count,
            health_score,
            top_recommendations,
        }
    }

    /// Format findings into a human-readable report
    pub fn format_report(&self, findings: &[DiagnosticFinding]) -> String {
        if findings.is_empty() {
            return "✅ No issues detected. All systems operational.".to_string();
        }

        let summary = self.summarize(findings);
        let mut report = String::new();

        report.push_str(&format!(
            "🔍 Download Diagnostics Report\n{}\n",
            "=".repeat(40)
        ));
        report.push_str(&format!(
            "Health Score: {}/100 {}\n",
            summary.health_score,
            health_emoji(summary.health_score)
        ));
        report.push_str(&format!(
            "Findings: {} total ({} critical, {} errors, {} warnings, {} info)\n\n",
            summary.total_findings,
            summary.critical_count,
            summary.error_count,
            summary.warning_count,
            summary.info_count
        ));

        for (i, finding) in findings.iter().enumerate() {
            report.push_str(&format!(
                "{}. [{}] {} - {}\n",
                i + 1,
                finding.severity,
                finding.category,
                finding.title
            ));
            report.push_str(&format!("   {}\n", finding.description));

            if !finding.related_task_ids.is_empty() {
                report.push_str(&format!(
                    "   Related tasks: {}\n",
                    finding.related_task_ids.join(", ")
                ));
            }

            if !finding.recommendations.is_empty() {
                report.push_str("   Recommendations:\n");
                for (j, rec) in finding.recommendations.iter().enumerate() {
                    report.push_str(&format!("   {}. {}\n", j + 1, rec));
                }
            }
            report.push('\n');
        }

        if !summary.top_recommendations.is_empty() {
            report.push_str("🎯 Priority Actions:\n");
            for (i, rec) in summary.top_recommendations.iter().enumerate() {
                report.push_str(&format!("   {}. {}\n", i + 1, rec));
            }
        }

        report
    }

    // --- Private check methods ---

    fn check_network_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        if !input.network_connected {
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Network,
                severity: DiagnosticSeverity::Critical,
                title: "Network Disconnected".to_string(),
                description: "No network connectivity detected. All downloads will fail."
                    .to_string(),
                recommendations: vec![
                    "Check your network connection and router".to_string(),
                    "Verify WiFi or Ethernet cable is connected".to_string(),
                    "Try running: ping 8.8.8.8 to test connectivity".to_string(),
                ],
                related_task_ids: Vec::new(),
            });
        }

        if !input.dns_working {
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Dns,
                severity: DiagnosticSeverity::Critical,
                title: "DNS Resolution Failed".to_string(),
                description: "DNS resolution is not working. Cannot resolve hostnames.".to_string(),
                recommendations: vec![
                    "Check DNS settings (try 8.8.8.8 or 1.1.1.1)".to_string(),
                    "Flush DNS cache: sudo systemd-resolve --flush-caches".to_string(),
                    "Verify /etc/resolv.conf has valid nameserver entries".to_string(),
                ],
                related_task_ids: Vec::new(),
            });
        }

        // Check for stalled downloads (no progress for a long time)
        let stalled_tasks: Vec<&TaskDiagnosticData> = input
            .task_diagnostics
            .iter()
            .filter(|t| {
                t.state == "Downloading"
                    && t.secs_since_last_progress > self.config.stuck_task_threshold_secs
            })
            .collect();

        if !stalled_tasks.is_empty() && input.network_connected {
            let task_ids: Vec<String> = stalled_tasks.iter().map(|t| t.task_id.clone()).collect();
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Network,
                severity: DiagnosticSeverity::Warning,
                title: format!("{} Stalled Download(s)", stalled_tasks.len()),
                description: format!(
                    "{} task(s) have not made progress for over {} minutes. Server may be unresponsive.",
                    stalled_tasks.len(),
                    self.config.stuck_task_threshold_secs / 60
                ),
                recommendations: vec![
                    "Try pausing and resuming stalled downloads".to_string(),
                    "Check if the server is down or rate-limiting".to_string(),
                    "Consider adding mirror URLs for affected downloads".to_string(),
                ],
                related_task_ids: task_ids,
            });
        }

        findings
    }

    fn check_disk_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        if input.available_disk_bytes < self.config.min_disk_space_bytes {
            let severity = if input.available_disk_bytes < self.config.min_disk_space_bytes / 10 {
                DiagnosticSeverity::Critical
            } else {
                DiagnosticSeverity::Error
            };

            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Disk,
                severity,
                title: "Low Disk Space".to_string(),
                description: format!(
                    "Only {} MB available (minimum: {} MB)",
                    input.available_disk_bytes / 1_048_576,
                    self.config.min_disk_space_bytes / 1_048_576
                ),
                recommendations: vec![
                    "Free up disk space by removing unnecessary files".to_string(),
                    "Change download save path to a drive with more space".to_string(),
                    "Enable auto-cleanup to remove completed/failed tasks".to_string(),
                ],
                related_task_ids: Vec::new(),
            });
        }

        // Check for tasks with 0 bytes downloaded but large expected size
        let zero_progress_tasks: Vec<&TaskDiagnosticData> = input
            .task_diagnostics
            .iter()
            .filter(|t| {
                t.state == "Downloading"
                    && t.downloaded_bytes == 0
                    && t.total_size > 0
                    && t.age_secs > 60
            })
            .collect();

        if !zero_progress_tasks.is_empty() {
            let task_ids: Vec<String> = zero_progress_tasks
                .iter()
                .map(|t| t.task_id.clone())
                .collect();
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Disk,
                severity: DiagnosticSeverity::Warning,
                title: format!("{} Task(s) With Zero Progress", zero_progress_tasks.len()),
                description: format!(
                    "{} task(s) have been active for over 60 seconds but downloaded 0 bytes. Possible disk write failure.",
                    zero_progress_tasks.len()
                ),
                recommendations: vec![
                    "Check disk permissions on the save directory".to_string(),
                    "Verify the save path exists and is writable".to_string(),
                    "Check for disk I/O errors in system logs".to_string(),
                ],
                related_task_ids: task_ids,
            });
        }

        findings
    }

    fn check_performance_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        // Check for slow downloads
        if input.current_speed_bps > 0
            && input.current_speed_bps < self.config.slow_download_threshold_bps
        {
            let slow_tasks: Vec<&TaskDiagnosticData> = input
                .task_diagnostics
                .iter()
                .filter(|t| {
                    t.state == "Downloading"
                        && t.speed_bps > 0
                        && t.speed_bps < self.config.slow_download_threshold_bps
                })
                .collect();

            if !slow_tasks.is_empty() {
                let task_ids: Vec<String> = slow_tasks.iter().map(|t| t.task_id.clone()).collect();
                findings.push(DiagnosticFinding {
                    category: DiagnosticCategory::Performance,
                    severity: DiagnosticSeverity::Warning,
                    title: format!("{} Slow Download(s)", slow_tasks.len()),
                    description: format!(
                        "{} task(s) downloading below threshold ({} KB/s)",
                        slow_tasks.len(),
                        self.config.slow_download_threshold_bps / 1000
                    ),
                    recommendations: vec![
                        "Check if other applications are consuming bandwidth".to_string(),
                        "Consider limiting concurrent downloads to improve per-task speed"
                            .to_string(),
                        "Try adding mirror URLs for slow downloads".to_string(),
                        "Check if the server is throttling speed".to_string(),
                    ],
                    related_task_ids: task_ids,
                });
            }
        }

        // Check for too many concurrent downloads
        if input.max_concurrent > 0 && input.active_downloads >= input.max_concurrent {
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Performance,
                severity: DiagnosticSeverity::Info,
                title: "Maximum Concurrent Downloads Reached".to_string(),
                description: format!(
                    "Running {}/{} concurrent downloads. Queued tasks will wait.",
                    input.active_downloads, input.max_concurrent
                ),
                recommendations: vec![
                    "Increase max concurrent downloads if bandwidth allows".to_string(),
                    "Wait for active downloads to complete".to_string(),
                ],
                related_task_ids: Vec::new(),
            });
        }

        findings
    }

    fn check_queue_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        // Large queue with no active downloads
        if input.active_downloads == 0 && input.queued_downloads > 0 {
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Queue,
                severity: DiagnosticSeverity::Error,
                title: "Queue Not Starting".to_string(),
                description: format!(
                    "{} task(s) queued but none are active. Scheduler may be blocked.",
                    input.queued_downloads
                ),
                recommendations: vec![
                    "Check if downloads are blocked by a schedule window".to_string(),
                    "Verify network connectivity is available".to_string(),
                    "Check if a data cap has been reached".to_string(),
                    "Try manually starting a queued download".to_string(),
                ],
                related_task_ids: Vec::new(),
            });
        }

        // High failure rate
        if input.failed_downloads > 0 {
            let total = input.active_downloads + input.queued_downloads + input.failed_downloads;
            let failure_rate = input.failed_downloads as f64 / total as f64;

            if failure_rate > 0.5 && total >= 3 {
                findings.push(DiagnosticFinding {
                    category: DiagnosticCategory::Queue,
                    severity: DiagnosticSeverity::Error,
                    title: "High Failure Rate".to_string(),
                    description: format!(
                        "{}/{} downloads failed ({:.0}% failure rate)",
                        input.failed_downloads,
                        total,
                        failure_rate * 100.0
                    ),
                    recommendations: vec![
                        "Check error messages for common failure patterns".to_string(),
                        "Verify URLs are still valid and accessible".to_string(),
                        "Check if proxy settings are correct".to_string(),
                        "Review error recovery settings".to_string(),
                    ],
                    related_task_ids: Vec::new(),
                });
            }
        }

        // Tasks with excessive retries
        let high_retry_tasks: Vec<&TaskDiagnosticData> = input
            .task_diagnostics
            .iter()
            .filter(|t| t.retry_count >= self.config.max_retry_threshold)
            .collect();

        if !high_retry_tasks.is_empty() {
            let task_ids: Vec<String> =
                high_retry_tasks.iter().map(|t| t.task_id.clone()).collect();
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Queue,
                severity: DiagnosticSeverity::Warning,
                title: format!("{} Task(s) With Excessive Retries", high_retry_tasks.len()),
                description: format!(
                    "{} task(s) have been retried {}+ times",
                    high_retry_tasks.len(),
                    self.config.max_retry_threshold
                ),
                recommendations: vec![
                    "Check if the download server is permanently unavailable".to_string(),
                    "Try alternative mirror URLs".to_string(),
                    "Consider removing permanently failed tasks".to_string(),
                ],
                related_task_ids: task_ids,
            });
        }

        findings
    }

    fn check_proxy_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        if input.proxy_configured {
            if let Some(reachable) = input.proxy_reachable {
                if !reachable {
                    findings.push(DiagnosticFinding {
                        category: DiagnosticCategory::Proxy,
                        severity: DiagnosticSeverity::Critical,
                        title: "Proxy Unreachable".to_string(),
                        description:
                            "Proxy is configured but not reachable. All downloads will fail."
                                .to_string(),
                        recommendations: vec![
                            "Check proxy server is running and accessible".to_string(),
                            "Verify proxy address and port settings".to_string(),
                            "Check proxy authentication credentials".to_string(),
                            "Try disabling proxy temporarily to confirm".to_string(),
                        ],
                        related_task_ids: Vec::new(),
                    });
                }
            }
        }

        findings
    }

    fn check_server_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        // Check for tasks with consecutive failures
        let failing_tasks: Vec<&TaskDiagnosticData> = input
            .task_diagnostics
            .iter()
            .filter(|t| t.consecutive_failures >= self.config.max_consecutive_failures)
            .collect();

        if !failing_tasks.is_empty() {
            let task_ids: Vec<String> = failing_tasks.iter().map(|t| t.task_id.clone()).collect();
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Server,
                severity: DiagnosticSeverity::Error,
                title: format!("{} Task(s) With Consecutive Failures", failing_tasks.len()),
                description: format!(
                    "{} task(s) have {}+ consecutive failures. Server may be down or blocking.",
                    failing_tasks.len(),
                    self.config.max_consecutive_failures
                ),
                recommendations: vec![
                    "Verify the download URL is still valid".to_string(),
                    "Check if the server requires authentication".to_string(),
                    "Try accessing the URL in a browser to confirm availability".to_string(),
                    "Consider using a different mirror or source".to_string(),
                ],
                related_task_ids: task_ids,
            });
        }

        findings
    }

    fn check_config_issues(&self, input: &DiagnosticsInput) -> Vec<DiagnosticFinding> {
        let mut findings = Vec::new();

        // Check for unreasonable max_concurrent
        if input.max_concurrent == 0 && input.queued_downloads > 20 {
            findings.push(DiagnosticFinding {
                category: DiagnosticCategory::Configuration,
                severity: DiagnosticSeverity::Info,
                title: "Unlimited Concurrent Downloads".to_string(),
                description: format!(
                    "No concurrent download limit set. {} tasks running simultaneously may impact performance.",
                    input.active_downloads
                ),
                recommendations: vec![
                    "Consider setting a max concurrent limit (e.g., 5-10)".to_string(),
                    "Unlimited concurrent downloads may overwhelm your network".to_string(),
                ],
                related_task_ids: Vec::new(),
            });
        }

        findings
    }

    fn calculate_health_score(&self, findings: &[DiagnosticFinding]) -> u32 {
        let mut score: i32 = 100;

        for finding in findings {
            match finding.severity {
                DiagnosticSeverity::Critical => score -= 25,
                DiagnosticSeverity::Error => score -= 15,
                DiagnosticSeverity::Warning => score -= 5,
                DiagnosticSeverity::Info => score -= 1,
            }
        }

        score.max(0) as u32
    }
}

/// Helper function for health score emoji
fn health_emoji(score: u32) -> &'static str {
    match score {
        90..=100 => "🟢 Excellent",
        70..=89 => "🟡 Good",
        50..=69 => "🟠 Fair",
        20..=49 => "🔴 Poor",
        _ => "💀 Critical",
    }
}

/// Save diagnostics config to disk
pub fn save_diagnostics_config(
    config: &DiagnosticsConfig,
    data_dir: &std::path::Path,
) -> Result<(), String> {
    let path = data_dir.join("download_diagnostics_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| format!("Failed to serialize config: {}", e))?;
    std::fs::write(&path, json).map_err(|e| format!("Failed to write config: {}", e))?;
    Ok(())
}

/// Load diagnostics config from disk
pub fn load_diagnostics_config(data_dir: &std::path::Path) -> Option<DiagnosticsConfig> {
    let path = data_dir.join("download_diagnostics_config.json");
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_input() -> DiagnosticsInput {
        DiagnosticsInput {
            current_speed_bps: 500_000,
            avg_speed_bps: 600_000,
            available_disk_bytes: 10_737_418_240, // 10 GB
            total_disk_bytes: 107_374_182_400,
            network_connected: true,
            dns_working: true,
            proxy_configured: false,
            proxy_reachable: None,
            active_downloads: 3,
            queued_downloads: 2,
            failed_downloads: 0,
            stalled_downloads: 0,
            max_concurrent: 5,
            task_diagnostics: Vec::new(),
        }
    }

    fn make_task(id: &str, state: &str, speed: u64, secs_since: u64) -> TaskDiagnosticData {
        TaskDiagnosticData {
            task_id: id.to_string(),
            task_name: format!("Task {}", id),
            state: state.to_string(),
            speed_bps: speed,
            progress_percent: 50.0,
            secs_since_last_progress: secs_since,
            retry_count: 0,
            consecutive_failures: 0,
            last_error: None,
            age_secs: 3600,
            total_size: 1_000_000_000,
            downloaded_bytes: 500_000_000,
        }
    }

    #[test]
    fn test_no_issues_clean_system() {
        let diag = DownloadDiagnostics::new();
        let input = default_input();
        let findings = diag.analyze(&input);
        assert!(
            findings.is_empty(),
            "Expected no findings for clean system, got {}",
            findings.len()
        );
    }

    #[test]
    fn test_network_disconnected() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        assert!(!findings.is_empty());

        let network_finding = findings
            .iter()
            .find(|f| {
                f.category == DiagnosticCategory::Network
                    && f.severity == DiagnosticSeverity::Critical
            })
            .expect("Should have critical network finding");
        assert!(network_finding.title.contains("Network Disconnected"));
    }

    #[test]
    fn test_dns_failure() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.dns_working = false;

        let findings = diag.analyze(&input);
        let dns_finding = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Dns)
            .expect("Should have DNS finding");
        assert_eq!(dns_finding.severity, DiagnosticSeverity::Critical);
    }

    #[test]
    fn test_low_disk_space() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.available_disk_bytes = 500_000_000; // 500 MB (below 1 GB threshold)

        let findings = diag.analyze(&input);
        let disk_finding = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Disk)
            .expect("Should have disk finding");
        assert!(disk_finding.severity >= DiagnosticSeverity::Error);
    }

    #[test]
    fn test_critical_disk_space() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.available_disk_bytes = 50_000_000; // 50 MB (below 10% of 1 GB)

        let findings = diag.analyze(&input);
        let disk_finding = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Disk)
            .expect("Should have disk finding");
        assert_eq!(disk_finding.severity, DiagnosticSeverity::Critical);
    }

    #[test]
    fn test_stalled_downloads() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 2000)]; // 33 min no progress

        let findings = diag.analyze(&input);
        let stalled = findings
            .iter()
            .find(|f| f.title.contains("Stalled"))
            .expect("Should have stalled finding");
        assert_eq!(stalled.severity, DiagnosticSeverity::Warning);
        assert_eq!(stalled.related_task_ids, vec!["t1"]);
    }

    #[test]
    fn test_slow_downloads() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.current_speed_bps = 5_000; // 5 KB/s (below 10 KB/s threshold)
        input.task_diagnostics = vec![make_task("t1", "Downloading", 5_000, 10)];

        let findings = diag.analyze(&input);
        let slow = findings
            .iter()
            .find(|f| f.title.contains("Slow"))
            .expect("Should have slow download finding");
        assert_eq!(slow.severity, DiagnosticSeverity::Warning);
    }

    #[test]
    fn test_queue_not_starting() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 0;
        input.queued_downloads = 5;

        let findings = diag.analyze(&input);
        let queue = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Queue)
            .expect("Should have queue finding");
        assert_eq!(queue.severity, DiagnosticSeverity::Error);
    }

    #[test]
    fn test_high_failure_rate() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 2;
        input.queued_downloads = 1;
        input.failed_downloads = 5;

        let findings = diag.analyze(&input);
        let failure = findings
            .iter()
            .find(|f| f.title.contains("Failure Rate"))
            .expect("Should have failure rate finding");
        assert_eq!(failure.severity, DiagnosticSeverity::Error);
    }

    #[test]
    fn test_proxy_unreachable() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.proxy_configured = true;
        input.proxy_reachable = Some(false);

        let findings = diag.analyze(&input);
        let proxy = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Proxy)
            .expect("Should have proxy finding");
        assert_eq!(proxy.severity, DiagnosticSeverity::Critical);
    }

    #[test]
    fn test_consecutive_failures() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.consecutive_failures = 5;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let server = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Server)
            .expect("Should have server finding");
        assert_eq!(server.severity, DiagnosticSeverity::Error);
    }

    #[test]
    fn test_excessive_retries() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 100_000, 10);
        task.retry_count = 10;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let retry = findings
            .iter()
            .find(|f| f.title.contains("Retries"))
            .expect("Should have retry finding");
        assert_eq!(retry.severity, DiagnosticSeverity::Warning);
    }

    #[test]
    fn test_summary_calculation() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;
        input.available_disk_bytes = 100_000_000;

        let findings = diag.analyze(&input);
        let summary = diag.summarize(&findings);

        assert!(summary.total_findings >= 2);
        assert!(summary.critical_count >= 1);
        assert!(summary.health_score < 100);
        assert!(!summary.top_recommendations.is_empty());
    }

    #[test]
    fn test_health_score_calculation() {
        let diag = DownloadDiagnostics::new();

        // No findings = 100
        let score = diag.calculate_health_score(&[]);
        assert_eq!(score, 100);

        // One critical = 75
        let findings = vec![DiagnosticFinding {
            category: DiagnosticCategory::Network,
            severity: DiagnosticSeverity::Critical,
            title: "Test".to_string(),
            description: "Test".to_string(),
            recommendations: vec![],
            related_task_ids: vec![],
        }];
        let score = diag.calculate_health_score(&findings);
        assert_eq!(score, 75);
    }

    #[test]
    fn test_format_report_empty() {
        let diag = DownloadDiagnostics::new();
        let report = diag.format_report(&[]);
        assert!(report.contains("No issues detected"));
    }

    #[test]
    fn test_format_report_with_findings() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        let report = diag.format_report(&findings);
        assert!(report.contains("Download Diagnostics Report"));
        assert!(report.contains("Network Disconnected"));
    }

    #[test]
    fn test_config_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = DiagnosticsConfig {
            enabled: false,
            slow_download_threshold_bps: 50_000,
            ..Default::default()
        };

        save_diagnostics_config(&config, temp_dir.path()).unwrap();
        let loaded = load_diagnostics_config(temp_dir.path()).unwrap();

        assert!(!loaded.enabled);
        assert_eq!(loaded.slow_download_threshold_bps, 50_000);
    }

    #[test]
    fn test_config_load_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let loaded = load_diagnostics_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_disabled_diagnostics() {
        let mut diag = DownloadDiagnostics::new();
        diag.set_config(DiagnosticsConfig {
            enabled: false,
            ..Default::default()
        });

        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        assert!(
            findings.is_empty(),
            "Disabled diagnostics should return no findings"
        );
    }

    #[test]
    fn test_max_findings_per_category() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        // Create many stalled tasks
        input.task_diagnostics = (0..20)
            .map(|i| make_task(&format!("t{}", i), "Downloading", 0, 2000))
            .collect();

        let findings = diag.analyze(&input);
        let network_findings: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Network)
            .collect();
        assert!(
            network_findings.len() <= 10,
            "Should limit findings per category"
        );
    }

    #[test]
    fn test_zero_progress_tasks() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.downloaded_bytes = 0;
        task.age_secs = 120; // 2 minutes old
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let disk = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Disk && f.title.contains("Zero Progress"))
            .expect("Should have zero progress finding");
        assert_eq!(disk.severity, DiagnosticSeverity::Warning);
    }

    #[test]
    fn test_unlimited_concurrent_info() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.max_concurrent = 0;
        input.active_downloads = 25;
        input.queued_downloads = 30;

        let findings = diag.analyze(&input);
        let config_finding = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Configuration)
            .expect("Should have configuration finding");
        assert_eq!(config_finding.severity, DiagnosticSeverity::Info);
    }

    #[test]
    fn test_severity_ordering() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false; // Critical
        input.available_disk_bytes = 500_000_000; // Error

        let findings = diag.analyze(&input);
        // Findings should be sorted by severity (critical first)
        if findings.len() >= 2 {
            assert!(findings[0].severity >= findings[1].severity);
        }
    }

    #[test]
    fn test_category_display() {
        assert_eq!(format!("{}", DiagnosticCategory::Network), "Network");
        assert_eq!(format!("{}", DiagnosticCategory::Dns), "DNS");
        assert_eq!(format!("{}", DiagnosticCategory::Disk), "Disk");
        assert_eq!(format!("{}", DiagnosticCategory::Server), "Server");
        assert_eq!(
            format!("{}", DiagnosticCategory::Configuration),
            "Configuration"
        );
        assert_eq!(
            format!("{}", DiagnosticCategory::Performance),
            "Performance"
        );
        assert_eq!(format!("{}", DiagnosticCategory::Proxy), "Proxy");
        assert_eq!(format!("{}", DiagnosticCategory::Queue), "Queue");
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(format!("{}", DiagnosticSeverity::Info), "INFO");
        assert_eq!(format!("{}", DiagnosticSeverity::Warning), "WARNING");
        assert_eq!(format!("{}", DiagnosticSeverity::Error), "ERROR");
        assert_eq!(format!("{}", DiagnosticSeverity::Critical), "CRITICAL");
    }

    #[test]
    fn test_health_emoji() {
        assert!(health_emoji(95).contains("Excellent"));
        assert!(health_emoji(75).contains("Good"));
        assert!(health_emoji(55).contains("Fair"));
        assert!(health_emoji(30).contains("Poor"));
        assert!(health_emoji(10).contains("Critical"));
    }

    #[test]
    fn test_multiple_issues_combined() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = true;
        input.dns_working = false; // Critical DNS
        input.available_disk_bytes = 100_000_000; // Error disk
        input.proxy_configured = true;
        input.proxy_reachable = Some(false); // Critical proxy
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 2000)]; // Warning stalled

        let findings = diag.analyze(&input);
        let summary = diag.summarize(&findings);

        assert!(summary.total_findings >= 4);
        assert!(summary.critical_count >= 2);
        assert!(summary.health_score < 50);
    }
}
