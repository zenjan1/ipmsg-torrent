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

    // ========== Serialization tests ==========

    #[test]
    fn test_diagnostic_category_serde_roundtrip() {
        let categories = vec![
            DiagnosticCategory::Network,
            DiagnosticCategory::Dns,
            DiagnosticCategory::Disk,
            DiagnosticCategory::Server,
            DiagnosticCategory::Configuration,
            DiagnosticCategory::Performance,
            DiagnosticCategory::Proxy,
            DiagnosticCategory::Queue,
        ];
        for cat in categories {
            let json = serde_json::to_string(&cat).unwrap();
            let back: DiagnosticCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(cat, back);
        }
    }

    #[test]
    fn test_diagnostic_category_serde_snake_case() {
        // Verify serde uses default (lowercase) representation
        let json = serde_json::to_string(&DiagnosticCategory::Network).unwrap();
        assert_eq!(json, "\"network\"");
        let json = serde_json::to_string(&DiagnosticCategory::Dns).unwrap();
        assert_eq!(json, "\"dns\"");
    }

    #[test]
    fn test_diagnostic_severity_serde_roundtrip() {
        let severities = vec![
            DiagnosticSeverity::Info,
            DiagnosticSeverity::Warning,
            DiagnosticSeverity::Error,
            DiagnosticSeverity::Critical,
        ];
        for sev in severities {
            let json = serde_json::to_string(&sev).unwrap();
            let back: DiagnosticSeverity = serde_json::from_str(&json).unwrap();
            assert_eq!(sev, back);
        }
    }

    #[test]
    fn test_diagnostic_severity_serde_lowercase() {
        let json = serde_json::to_string(&DiagnosticSeverity::Critical).unwrap();
        assert_eq!(json, "\"critical\"");
    }

    #[test]
    fn test_diagnostic_finding_serde_roundtrip() {
        let finding = DiagnosticFinding {
            category: DiagnosticCategory::Network,
            severity: DiagnosticSeverity::Critical,
            title: "Test Title".to_string(),
            description: "Test Description".to_string(),
            recommendations: vec!["Rec 1".to_string(), "Rec 2".to_string()],
            related_task_ids: vec!["t1".to_string(), "t2".to_string()],
        };
        let json = serde_json::to_string(&finding).unwrap();
        let back: DiagnosticFinding = serde_json::from_str(&json).unwrap();
        assert_eq!(back.category, DiagnosticCategory::Network);
        assert_eq!(back.severity, DiagnosticSeverity::Critical);
        assert_eq!(back.title, "Test Title");
        assert_eq!(back.recommendations.len(), 2);
        assert_eq!(back.related_task_ids.len(), 2);
    }

    #[test]
    fn test_diagnostic_finding_serde_empty_vecs() {
        let finding = DiagnosticFinding {
            category: DiagnosticCategory::Disk,
            severity: DiagnosticSeverity::Info,
            title: "T".to_string(),
            description: "D".to_string(),
            recommendations: vec![],
            related_task_ids: vec![],
        };
        let json = serde_json::to_string(&finding).unwrap();
        let back: DiagnosticFinding = serde_json::from_str(&json).unwrap();
        assert!(back.recommendations.is_empty());
        assert!(back.related_task_ids.is_empty());
    }

    #[test]
    fn test_diagnostics_config_serde_roundtrip() {
        let config = DiagnosticsConfig {
            enabled: false,
            slow_download_threshold_bps: 50_000,
            stuck_task_threshold_secs: 600,
            min_disk_space_bytes: 2_000_000_000,
            max_retry_threshold: 10,
            max_consecutive_failures: 5,
            check_network: false,
            check_disk: true,
            check_performance: false,
            check_queue: true,
            max_findings_per_category: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: DiagnosticsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.enabled, false);
        assert_eq!(back.slow_download_threshold_bps, 50_000);
        assert_eq!(back.stuck_task_threshold_secs, 600);
        assert_eq!(back.min_disk_space_bytes, 2_000_000_000);
        assert_eq!(back.max_retry_threshold, 10);
        assert_eq!(back.max_consecutive_failures, 5);
        assert_eq!(back.check_network, false);
        assert_eq!(back.check_disk, true);
        assert_eq!(back.check_performance, false);
        assert_eq!(back.check_queue, true);
        assert_eq!(back.max_findings_per_category, 5);
    }

    #[test]
    fn test_diagnostics_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"slow_download_threshold_bps":10000,"stuck_task_threshold_secs":1800,"min_disk_space_bytes":1073741824,"max_retry_threshold":5,"max_consecutive_failures":3,"check_network":true,"check_disk":true,"check_performance":true,"check_queue":true,"max_findings_per_category":10,"unknown_field":"value"}"#;
        let config: DiagnosticsConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_task_diagnostic_data_serde_roundtrip() {
        let task = TaskDiagnosticData {
            task_id: "t1".to_string(),
            task_name: "Test Task".to_string(),
            state: "Downloading".to_string(),
            speed_bps: 100_000,
            progress_percent: 42.5,
            secs_since_last_progress: 30,
            retry_count: 3,
            consecutive_failures: 1,
            last_error: Some("Connection reset".to_string()),
            age_secs: 7200,
            total_size: 1_000_000_000,
            downloaded_bytes: 425_000_000,
        };
        let json = serde_json::to_string(&task).unwrap();
        let back: TaskDiagnosticData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, "t1");
        assert_eq!(back.progress_percent, 42.5);
        assert_eq!(back.last_error, Some("Connection reset".to_string()));
    }

    #[test]
    fn test_task_diagnostic_data_serde_none_error() {
        let task = TaskDiagnosticData {
            task_id: "t1".to_string(),
            task_name: "Test".to_string(),
            state: "Downloading".to_string(),
            speed_bps: 0,
            progress_percent: 0.0,
            secs_since_last_progress: 0,
            retry_count: 0,
            consecutive_failures: 0,
            last_error: None,
            age_secs: 0,
            total_size: 0,
            downloaded_bytes: 0,
        };
        let json = serde_json::to_string(&task).unwrap();
        let back: TaskDiagnosticData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.last_error, None);
    }

    #[test]
    fn test_diagnostics_summary_serde_roundtrip() {
        let mut summary = DiagnosticsSummary {
            total_findings: 5,
            findings_by_severity: HashMap::new(),
            findings_by_category: HashMap::new(),
            critical_count: 1,
            error_count: 2,
            warning_count: 1,
            info_count: 1,
            health_score: 65,
            top_recommendations: vec!["Fix network".to_string()],
        };
        summary
            .findings_by_severity
            .insert("CRITICAL".to_string(), 1);
        summary
            .findings_by_category
            .insert("Network".to_string(), 3);

        let json = serde_json::to_string(&summary).unwrap();
        let back: DiagnosticsSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_findings, 5);
        assert_eq!(back.critical_count, 1);
        assert_eq!(back.health_score, 65);
        assert_eq!(back.top_recommendations.len(), 1);
    }

    #[test]
    fn test_download_diagnostics_serde_roundtrip() {
        let diag = DownloadDiagnostics::with_config(DiagnosticsConfig {
            enabled: false,
            slow_download_threshold_bps: 99_999,
            ..Default::default()
        });
        let json = serde_json::to_string(&diag).unwrap();
        let back: DownloadDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(back.get_config().enabled, false);
        assert_eq!(back.get_config().slow_download_threshold_bps, 99_999);
    }

    // ========== Default value tests ==========

    #[test]
    fn test_diagnostics_config_default_values() {
        let config = DiagnosticsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.slow_download_threshold_bps, 10_000);
        assert_eq!(config.stuck_task_threshold_secs, 1800);
        assert_eq!(config.min_disk_space_bytes, 1_073_741_824);
        assert_eq!(config.max_retry_threshold, 5);
        assert_eq!(config.max_consecutive_failures, 3);
        assert!(config.check_network);
        assert!(config.check_disk);
        assert!(config.check_performance);
        assert!(config.check_queue);
        assert_eq!(config.max_findings_per_category, 10);
    }

    #[test]
    fn test_download_diagnostics_default_equals_new() {
        let diag_default = DownloadDiagnostics::default();
        let diag_new = DownloadDiagnostics::new();
        assert_eq!(
            diag_default.get_config().enabled,
            diag_new.get_config().enabled
        );
        assert_eq!(
            diag_default.get_config().slow_download_threshold_bps,
            diag_new.get_config().slow_download_threshold_bps
        );
    }

    #[test]
    fn test_diagnostics_input_default() {
        let input = DiagnosticsInput::default();
        assert_eq!(input.current_speed_bps, 0);
        assert_eq!(input.avg_speed_bps, 0);
        assert_eq!(input.available_disk_bytes, 0);
        assert_eq!(input.total_disk_bytes, 0);
        assert!(!input.network_connected);
        assert!(!input.dns_working);
        assert!(!input.proxy_configured);
        assert_eq!(input.proxy_reachable, None);
        assert_eq!(input.active_downloads, 0);
        assert_eq!(input.queued_downloads, 0);
        assert_eq!(input.failed_downloads, 0);
        assert_eq!(input.stalled_downloads, 0);
        assert_eq!(input.max_concurrent, 0);
        assert!(input.task_diagnostics.is_empty());
    }

    // ========== Clone/Debug trait tests ==========

    #[test]
    fn test_diagnostic_category_clone_copy_debug() {
        let cat = DiagnosticCategory::Network;
        let cloned = cat.clone();
        assert_eq!(cat, cloned);
        // Copy trait
        let copied = cat;
        assert_eq!(copied, DiagnosticCategory::Network);
        // Debug trait
        let debug_str = format!("{:?}", cat);
        assert_eq!(debug_str, "Network");
    }

    #[test]
    fn test_diagnostic_severity_clone_copy_debug() {
        let sev = DiagnosticSeverity::Critical;
        let cloned = sev.clone();
        assert_eq!(sev, cloned);
        let copied = sev;
        assert_eq!(copied, DiagnosticSeverity::Critical);
        let debug_str = format!("{:?}", sev);
        assert_eq!(debug_str, "Critical");
    }

    #[test]
    fn test_diagnostic_finding_clone_debug() {
        let finding = DiagnosticFinding {
            category: DiagnosticCategory::Disk,
            severity: DiagnosticSeverity::Error,
            title: "T".to_string(),
            description: "D".to_string(),
            recommendations: vec!["R".to_string()],
            related_task_ids: vec!["t1".to_string()],
        };
        let cloned = finding.clone();
        assert_eq!(cloned.title, "T");
        assert_eq!(cloned.recommendations.len(), 1);
        let debug_str = format!("{:?}", finding);
        assert!(debug_str.contains("Disk"));
    }

    #[test]
    fn test_diagnostics_config_clone_debug() {
        let config = DiagnosticsConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("DiagnosticsConfig"));
    }

    #[test]
    fn test_task_diagnostic_data_clone_debug() {
        let task = make_task("t1", "Downloading", 100_000, 10);
        let cloned = task.clone();
        assert_eq!(cloned.task_id, "t1");
        let debug_str = format!("{:?}", task);
        assert!(debug_str.contains("TaskDiagnosticData"));
    }

    #[test]
    fn test_diagnostics_summary_clone_debug() {
        let summary = DiagnosticsSummary {
            total_findings: 0,
            findings_by_severity: HashMap::new(),
            findings_by_category: HashMap::new(),
            critical_count: 0,
            error_count: 0,
            warning_count: 0,
            info_count: 0,
            health_score: 100,
            top_recommendations: vec![],
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_findings, 0);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("DiagnosticsSummary"));
    }

    #[test]
    fn test_download_diagnostics_clone_debug() {
        let diag = DownloadDiagnostics::new();
        let cloned = diag.clone();
        assert_eq!(cloned.get_config().enabled, diag.get_config().enabled);
        let debug_str = format!("{:?}", diag);
        assert!(debug_str.contains("DownloadDiagnostics"));
    }

    // ========== PartialOrd/Ord for severity ==========

    #[test]
    fn test_severity_partial_ord() {
        assert!(DiagnosticSeverity::Critical > DiagnosticSeverity::Error);
        assert!(DiagnosticSeverity::Error > DiagnosticSeverity::Warning);
        assert!(DiagnosticSeverity::Warning > DiagnosticSeverity::Info);
    }

    #[test]
    fn test_severity_partial_ord_equal() {
        assert_eq!(
            DiagnosticSeverity::Warning.partial_cmp(&DiagnosticSeverity::Warning),
            Some(std::cmp::Ordering::Equal)
        );
    }

    // ========== Eq/Hash for category ==========

    #[test]
    fn test_diagnostic_category_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(DiagnosticCategory::Network);
        set.insert(DiagnosticCategory::Dns);
        set.insert(DiagnosticCategory::Network); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_diagnostic_category_eq() {
        assert_eq!(DiagnosticCategory::Network, DiagnosticCategory::Network);
        assert_ne!(DiagnosticCategory::Network, DiagnosticCategory::Dns);
    }

    // ========== Constructor and config tests ==========

    #[test]
    fn test_with_config() {
        let config = DiagnosticsConfig {
            enabled: false,
            slow_download_threshold_bps: 123_456,
            ..Default::default()
        };
        let diag = DownloadDiagnostics::with_config(config);
        assert!(!diag.get_config().enabled);
        assert_eq!(diag.get_config().slow_download_threshold_bps, 123_456);
    }

    #[test]
    fn test_set_config_updates() {
        let mut diag = DownloadDiagnostics::new();
        assert!(diag.get_config().enabled);

        diag.set_config(DiagnosticsConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(!diag.get_config().enabled);
    }

    #[test]
    fn test_get_config_returns_reference() {
        let diag = DownloadDiagnostics::new();
        let config = diag.get_config();
        assert!(config.enabled);
    }

    // ========== Analyze with disabled checks ==========

    #[test]
    fn test_check_network_disabled_still_detects_stalled() {
        // check_network=false should skip network disconnected check
        // but stalled tasks are in check_network_issues, so they should also be skipped
        let mut diag = DownloadDiagnostics::new();
        diag.set_config(DiagnosticsConfig {
            check_network: false,
            ..Default::default()
        });
        let mut input = default_input();
        input.network_connected = false; // Would trigger critical
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 2000)]; // stalled

        let findings = diag.analyze(&input);
        // No network findings because check_network=false
        let network: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Network)
            .collect();
        assert!(network.is_empty());
    }

    #[test]
    fn test_check_disk_disabled() {
        let mut diag = DownloadDiagnostics::new();
        diag.set_config(DiagnosticsConfig {
            check_disk: false,
            ..Default::default()
        });
        let mut input = default_input();
        input.available_disk_bytes = 0; // Would trigger critical disk

        let findings = diag.analyze(&input);
        let disk: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Disk)
            .collect();
        assert!(disk.is_empty());
    }

    #[test]
    fn test_check_performance_disabled() {
        let mut diag = DownloadDiagnostics::new();
        diag.set_config(DiagnosticsConfig {
            check_performance: false,
            ..Default::default()
        });
        let mut input = default_input();
        input.current_speed_bps = 1_000; // Would trigger slow download

        let findings = diag.analyze(&input);
        let perf: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Performance)
            .collect();
        assert!(perf.is_empty());
    }

    #[test]
    fn test_check_queue_disabled() {
        let mut diag = DownloadDiagnostics::new();
        diag.set_config(DiagnosticsConfig {
            check_queue: false,
            ..Default::default()
        });
        let mut input = default_input();
        input.active_downloads = 0;
        input.queued_downloads = 10; // Would trigger queue not starting

        let findings = diag.analyze(&input);
        let queue: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Queue)
            .collect();
        assert!(queue.is_empty());
    }

    // ========== Boundary tests ==========

    #[test]
    fn test_disk_space_exact_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        // Exactly at threshold (1 GB) - should NOT trigger
        input.available_disk_bytes = 1_073_741_824;

        let findings = diag.analyze(&input);
        let disk: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Disk && f.title.contains("Low Disk"))
            .collect();
        assert!(disk.is_empty(), "Exactly at threshold should not trigger");
    }

    #[test]
    fn test_disk_space_one_below_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.available_disk_bytes = 1_073_741_823; // 1 byte below 1 GB

        let findings = diag.analyze(&input);
        let disk: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Disk && f.title.contains("Low Disk"))
            .collect();
        assert_eq!(disk.len(), 1);
    }

    #[test]
    fn test_disk_critical_at_10_percent_boundary() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        // Exactly 10% of 1GB = 107374182.4 bytes
        input.available_disk_bytes = 107_374_182; // Just below 10%

        let findings = diag.analyze(&input);
        let disk = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Disk && f.title.contains("Low Disk"))
            .expect("Should have disk finding");
        assert_eq!(disk.severity, DiagnosticSeverity::Critical);
    }

    #[test]
    fn test_slow_download_exact_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.current_speed_bps = 10_000; // Exactly at threshold
        input.task_diagnostics = vec![make_task("t1", "Downloading", 10_000, 10)];

        let findings = diag.analyze(&input);
        // Speed is NOT < threshold, so no slow finding
        let slow: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Slow"))
            .collect();
        assert!(slow.is_empty(), "Exactly at threshold should not trigger");
    }

    #[test]
    fn test_slow_download_one_below_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.current_speed_bps = 9_999; // Just below threshold
        input.task_diagnostics = vec![make_task("t1", "Downloading", 9_999, 10)];

        let findings = diag.analyze(&input);
        let slow: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Slow"))
            .collect();
        assert_eq!(slow.len(), 1);
    }

    #[test]
    fn test_stalled_exact_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        // Exactly at threshold (1800s) - should NOT trigger
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 1800)];

        let findings = diag.analyze(&input);
        let stalled: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Stalled"))
            .collect();
        assert!(
            stalled.is_empty(),
            "Exactly at threshold should not trigger"
        );
    }

    #[test]
    fn test_stalled_one_above_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 1801)];

        let findings = diag.analyze(&input);
        let stalled: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Stalled"))
            .collect();
        assert_eq!(stalled.len(), 1);
    }

    #[test]
    fn test_stalled_not_triggered_when_network_disconnected() {
        // Stalled tasks should only be flagged when network IS connected
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 2000)];

        let findings = diag.analyze(&input);
        let stalled: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Stalled"))
            .collect();
        assert!(
            stalled.is_empty(),
            "Should not flag stalled when network disconnected"
        );
    }

    // ========== Zero progress edge cases ==========

    #[test]
    fn test_zero_progress_task_with_zero_total_size() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.downloaded_bytes = 0;
        task.total_size = 0; // Unknown size
        task.age_secs = 120;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let zero: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Zero Progress"))
            .collect();
        assert!(
            zero.is_empty(),
            "Should not flag zero progress when total_size=0"
        );
    }

    #[test]
    fn test_zero_progress_task_young() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.downloaded_bytes = 0;
        task.total_size = 1_000_000;
        task.age_secs = 30; // Only 30 seconds old (< 60s threshold)
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let zero: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Zero Progress"))
            .collect();
        assert!(
            zero.is_empty(),
            "Should not flag zero progress for young tasks"
        );
    }

    #[test]
    fn test_zero_progress_non_downloading_state() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Paused", 0, 10);
        task.downloaded_bytes = 0;
        task.total_size = 1_000_000;
        task.age_secs = 120;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let zero: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Zero Progress"))
            .collect();
        assert!(
            zero.is_empty(),
            "Should not flag zero progress for non-Downloading state"
        );
    }

    // ========== Failure rate edge cases ==========

    #[test]
    fn test_failure_rate_exactly_50_percent() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 1;
        input.queued_downloads = 1;
        input.failed_downloads = 2; // 2/4 = 50%

        let findings = diag.analyze(&input);
        let failure: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Failure Rate"))
            .collect();
        assert!(
            failure.is_empty(),
            "Exactly 50% should not trigger (>50% required)"
        );
    }

    #[test]
    fn test_failure_rate_above_50_percent() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 1;
        input.queued_downloads = 1;
        input.failed_downloads = 3; // 3/5 = 60%

        let findings = diag.analyze(&input);
        let failure: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Failure Rate"))
            .collect();
        assert_eq!(failure.len(), 1);
    }

    #[test]
    fn test_failure_rate_total_less_than_3() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 0;
        input.queued_downloads = 0;
        input.failed_downloads = 2; // 2/2 = 100% but total < 3

        let findings = diag.analyze(&input);
        let failure: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Failure Rate"))
            .collect();
        assert!(
            failure.is_empty(),
            "Total < 3 should not trigger failure rate"
        );
    }

    #[test]
    fn test_failure_rate_zero_failures() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 5;
        input.queued_downloads = 3;
        input.failed_downloads = 0;

        let findings = diag.analyze(&input);
        let failure: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Failure Rate"))
            .collect();
        assert!(failure.is_empty());
    }

    // ========== Proxy tests ==========

    #[test]
    fn test_proxy_not_configured_no_finding() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.proxy_configured = false;
        input.proxy_reachable = None;

        let findings = diag.analyze(&input);
        let proxy: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Proxy)
            .collect();
        assert!(proxy.is_empty());
    }

    #[test]
    fn test_proxy_configured_and_reachable() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.proxy_configured = true;
        input.proxy_reachable = Some(true);

        let findings = diag.analyze(&input);
        let proxy: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Proxy)
            .collect();
        assert!(
            proxy.is_empty(),
            "Reachable proxy should not trigger finding"
        );
    }

    #[test]
    fn test_proxy_configured_no_reachability_info() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.proxy_configured = true;
        input.proxy_reachable = None; // Unknown

        let findings = diag.analyze(&input);
        let proxy: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Proxy)
            .collect();
        assert!(
            proxy.is_empty(),
            "Unknown reachability should not trigger finding"
        );
    }

    // ========== Max concurrent edge cases ==========

    #[test]
    fn test_max_concurrent_reached_exactly() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 5;
        input.max_concurrent = 5;

        let findings = diag.analyze(&input);
        let max: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Maximum Concurrent"))
            .collect();
        assert_eq!(max.len(), 1);
        assert_eq!(max[0].severity, DiagnosticSeverity::Info);
    }

    #[test]
    fn test_max_concurrent_zero_with_small_queue() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.max_concurrent = 0;
        input.active_downloads = 5;
        input.queued_downloads = 10; // < 20, so no config finding

        let findings = diag.analyze(&input);
        let config: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Configuration)
            .collect();
        assert!(config.is_empty());
    }

    #[test]
    fn test_max_concurrent_zero_with_exactly_20_queued() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.max_concurrent = 0;
        input.active_downloads = 5;
        input.queued_downloads = 20; // Not > 20

        let findings = diag.analyze(&input);
        let config: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Configuration)
            .collect();
        assert!(config.is_empty());
    }

    #[test]
    fn test_max_concurrent_zero_with_over_20_queued() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.max_concurrent = 0;
        input.active_downloads = 5;
        input.queued_downloads = 21; // > 20

        let findings = diag.analyze(&input);
        let config: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Configuration)
            .collect();
        assert_eq!(config.len(), 1);
    }

    // ========== Format report tests ==========

    #[test]
    fn test_format_report_contains_health_score() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        let report = diag.format_report(&findings);
        assert!(report.contains("Health Score:"));
        assert!(report.contains("/100"));
    }

    #[test]
    fn test_format_report_contains_recommendations() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        let report = diag.format_report(&findings);
        assert!(report.contains("Recommendations:"));
        assert!(report.contains("Check your network"));
    }

    #[test]
    fn test_format_report_contains_related_tasks() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.task_diagnostics = vec![make_task("task-abc", "Downloading", 0, 2000)];

        let findings = diag.analyze(&input);
        let report = diag.format_report(&findings);
        assert!(report.contains("Related tasks: task-abc"));
    }

    #[test]
    fn test_format_report_contains_priority_actions() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        let report = diag.format_report(&findings);
        assert!(report.contains("Priority Actions:"));
    }

    #[test]
    fn test_format_report_no_priority_actions_when_only_info() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 5;
        input.max_concurrent = 5;

        let findings = diag.analyze(&input);
        let report = diag.format_report(&findings);
        // Only Info findings, so top_recommendations should be empty
        // Priority Actions section should not appear
        assert!(!report.contains("Priority Actions:"));
    }

    // ========== Summarize tests ==========

    #[test]
    fn test_summarize_empty_findings() {
        let diag = DownloadDiagnostics::new();
        let summary = diag.summarize(&[]);
        assert_eq!(summary.total_findings, 0);
        assert_eq!(summary.critical_count, 0);
        assert_eq!(summary.error_count, 0);
        assert_eq!(summary.warning_count, 0);
        assert_eq!(summary.info_count, 0);
        assert_eq!(summary.health_score, 100);
        assert!(summary.top_recommendations.is_empty());
    }

    #[test]
    fn test_summarize_recommendation_deduplication() {
        let diag = DownloadDiagnostics::new();
        let findings = vec![
            DiagnosticFinding {
                category: DiagnosticCategory::Network,
                severity: DiagnosticSeverity::Critical,
                title: "Net".to_string(),
                description: "D".to_string(),
                recommendations: vec!["Same advice".to_string()],
                related_task_ids: vec![],
            },
            DiagnosticFinding {
                category: DiagnosticCategory::Dns,
                severity: DiagnosticSeverity::Critical,
                title: "DNS".to_string(),
                description: "D".to_string(),
                recommendations: vec!["Same advice".to_string()],
                related_task_ids: vec![],
            },
        ];
        let summary = diag.summarize(&findings);
        // "Same advice" appears in both findings but should only appear once in all_recommendations
        let count = summary
            .top_recommendations
            .iter()
            .filter(|r| r.as_str() == "Same advice")
            .count();
        // top_recommendations comes from Error+ findings, deduped
        assert_eq!(count, 1);
    }

    #[test]
    fn test_summarize_findings_by_category() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false; // Critical Network
        input.dns_working = false; // Critical DNS

        let findings = diag.analyze(&input);
        let summary = diag.summarize(&findings);

        assert!(summary.findings_by_category.contains_key("Network"));
        assert!(summary.findings_by_category.contains_key("DNS"));
    }

    #[test]
    fn test_summarize_findings_by_severity() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false; // Critical

        let findings = diag.analyze(&input);
        let summary = diag.summarize(&findings);

        assert!(summary.findings_by_severity.contains_key("CRITICAL"));
    }

    // ========== Health score boundary tests ==========

    #[test]
    fn test_health_score_many_findings_clamps_to_zero() {
        let diag = DownloadDiagnostics::new();
        // 5 critical findings = 100 - 5*25 = -25, clamped to 0
        let findings: Vec<DiagnosticFinding> = (0..5)
            .map(|_| DiagnosticFinding {
                category: DiagnosticCategory::Network,
                severity: DiagnosticSeverity::Critical,
                title: "T".to_string(),
                description: "D".to_string(),
                recommendations: vec![],
                related_task_ids: vec![],
            })
            .collect();
        let score = diag.calculate_health_score(&findings);
        assert_eq!(score, 0);
    }

    #[test]
    fn test_health_score_single_info() {
        let diag = DownloadDiagnostics::new();
        let findings = vec![DiagnosticFinding {
            category: DiagnosticCategory::Performance,
            severity: DiagnosticSeverity::Info,
            title: "T".to_string(),
            description: "D".to_string(),
            recommendations: vec![],
            related_task_ids: vec![],
        }];
        let score = diag.calculate_health_score(&findings);
        assert_eq!(score, 99);
    }

    #[test]
    fn test_health_score_single_warning() {
        let diag = DownloadDiagnostics::new();
        let findings = vec![DiagnosticFinding {
            category: DiagnosticCategory::Performance,
            severity: DiagnosticSeverity::Warning,
            title: "T".to_string(),
            description: "D".to_string(),
            recommendations: vec![],
            related_task_ids: vec![],
        }];
        let score = diag.calculate_health_score(&findings);
        assert_eq!(score, 95);
    }

    #[test]
    fn test_health_score_single_error() {
        let diag = DownloadDiagnostics::new();
        let findings = vec![DiagnosticFinding {
            category: DiagnosticCategory::Queue,
            severity: DiagnosticSeverity::Error,
            title: "T".to_string(),
            description: "D".to_string(),
            recommendations: vec![],
            related_task_ids: vec![],
        }];
        let score = diag.calculate_health_score(&findings);
        assert_eq!(score, 85);
    }

    // ========== Health emoji boundary tests ==========

    #[test]
    fn test_health_emoji_boundaries() {
        assert!(health_emoji(90).contains("Excellent"));
        assert!(health_emoji(89).contains("Good"));
        assert!(health_emoji(70).contains("Good"));
        assert!(health_emoji(69).contains("Fair"));
        assert!(health_emoji(50).contains("Fair"));
        assert!(health_emoji(49).contains("Poor"));
        assert!(health_emoji(20).contains("Poor"));
        assert!(health_emoji(19).contains("Critical"));
        assert!(health_emoji(0).contains("Critical"));
    }

    // ========== Persistence tests ==========

    #[test]
    fn test_persistence_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Write first config
        let config1 = DiagnosticsConfig {
            enabled: true,
            slow_download_threshold_bps: 10_000,
            ..Default::default()
        };
        save_diagnostics_config(&config1, temp_dir.path()).unwrap();

        // Overwrite with second config
        let config2 = DiagnosticsConfig {
            enabled: false,
            slow_download_threshold_bps: 99_999,
            ..Default::default()
        };
        save_diagnostics_config(&config2, temp_dir.path()).unwrap();

        let loaded = load_diagnostics_config(temp_dir.path()).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.slow_download_threshold_bps, 99_999);
    }

    #[test]
    fn test_persistence_invalid_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("download_diagnostics_config.json");
        std::fs::write(&path, "not valid json {{{").unwrap();

        let loaded = load_diagnostics_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_persistence_all_config_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = DiagnosticsConfig {
            enabled: false,
            slow_download_threshold_bps: 77_777,
            stuck_task_threshold_secs: 999,
            min_disk_space_bytes: 5_000_000_000,
            max_retry_threshold: 20,
            max_consecutive_failures: 10,
            check_network: false,
            check_disk: false,
            check_performance: false,
            check_queue: false,
            max_findings_per_category: 3,
        };
        save_diagnostics_config(&config, temp_dir.path()).unwrap();
        let loaded = load_diagnostics_config(temp_dir.path()).unwrap();

        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.slow_download_threshold_bps, 77_777);
        assert_eq!(loaded.stuck_task_threshold_secs, 999);
        assert_eq!(loaded.min_disk_space_bytes, 5_000_000_000);
        assert_eq!(loaded.max_retry_threshold, 20);
        assert_eq!(loaded.max_consecutive_failures, 10);
        assert!(!loaded.check_network);
        assert!(!loaded.check_disk);
        assert!(!loaded.check_performance);
        assert!(!loaded.check_queue);
        assert_eq!(loaded.max_findings_per_category, 3);
    }

    // ========== Unicode tests ==========

    #[test]
    fn test_unicode_task_name_in_analysis() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 5_000, 10);
        task.task_name = "下载文件_测试_日本語".to_string();
        task.speed_bps = 5_000;
        input.current_speed_bps = 5_000;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let slow = findings
            .iter()
            .find(|f| f.title.contains("Slow"))
            .expect("Should detect slow download with Unicode task name");
        assert!(slow.related_task_ids.contains(&"t1".to_string()));
    }

    #[test]
    fn test_unicode_task_name_zero_progress() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t-emoji-🚀", "Downloading", 0, 10);
        task.task_name = "🎉文件🎉".to_string();
        task.downloaded_bytes = 0;
        task.total_size = 1_000_000;
        task.age_secs = 120;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let zero = findings
            .iter()
            .find(|f| f.title.contains("Zero Progress"))
            .expect("Should detect zero progress with Unicode task name");
        assert!(zero.related_task_ids.contains(&"t-emoji-🚀".to_string()));
    }

    // ========== Multiple slow tasks ==========

    #[test]
    fn test_multiple_slow_tasks() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.current_speed_bps = 5_000;
        input.task_diagnostics = vec![
            make_task("t1", "Downloading", 5_000, 10),
            make_task("t2", "Downloading", 3_000, 10),
            make_task("t3", "Downloading", 8_000, 10),
        ];

        let findings = diag.analyze(&input);
        let slow = findings
            .iter()
            .find(|f| f.title.contains("Slow"))
            .expect("Should detect slow downloads");
        assert_eq!(slow.related_task_ids.len(), 3);
    }

    // ========== Excessive retries boundary ==========

    #[test]
    fn test_excessive_retries_exact_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 100_000, 10);
        task.retry_count = 5; // Exactly at threshold
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let retry = findings
            .iter()
            .find(|f| f.title.contains("Retries"))
            .expect("Should flag at exact threshold");
        assert_eq!(retry.severity, DiagnosticSeverity::Warning);
    }

    #[test]
    fn test_excessive_retries_below_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 100_000, 10);
        task.retry_count = 4; // Below threshold (5)
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let retry: Vec<_> = findings
            .iter()
            .filter(|f| f.title.contains("Retries"))
            .collect();
        assert!(retry.is_empty(), "Below threshold should not trigger");
    }

    // ========== Consecutive failures boundary ==========

    #[test]
    fn test_consecutive_failures_exact_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.consecutive_failures = 3; // Exactly at threshold
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let server = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Server)
            .expect("Should flag at exact threshold");
        assert_eq!(server.severity, DiagnosticSeverity::Error);
    }

    #[test]
    fn test_consecutive_failures_below_threshold() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.consecutive_failures = 2; // Below threshold (3)
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let server: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Server)
            .collect();
        assert!(server.is_empty(), "Below threshold should not trigger");
    }

    // ========== Recommendation content verification ==========

    #[test]
    fn test_network_disconnected_recommendations() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.network_connected = false;

        let findings = diag.analyze(&input);
        let net = findings
            .iter()
            .find(|f| f.title.contains("Network Disconnected"))
            .unwrap();
        assert_eq!(net.recommendations.len(), 3);
        assert!(net.recommendations[0].contains("network connection"));
    }

    #[test]
    fn test_dns_failure_recommendations() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.dns_working = false;

        let findings = diag.analyze(&input);
        let dns = findings.iter().find(|f| f.title.contains("DNS")).unwrap();
        assert_eq!(dns.recommendations.len(), 3);
        assert!(dns.recommendations[0].contains("DNS settings"));
    }

    #[test]
    fn test_proxy_unreachable_recommendations() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.proxy_configured = true;
        input.proxy_reachable = Some(false);

        let findings = diag.analyze(&input);
        let proxy = findings
            .iter()
            .find(|f| f.title.contains("Proxy Unreachable"))
            .unwrap();
        assert_eq!(proxy.recommendations.len(), 4);
    }

    // ========== Server issues with multiple tasks ==========

    #[test]
    fn test_consecutive_failures_multiple_tasks() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        let mut t1 = make_task("t1", "Downloading", 0, 10);
        t1.consecutive_failures = 5;
        let mut t2 = make_task("t2", "Downloading", 0, 10);
        t2.consecutive_failures = 3;
        let t3 = make_task("t3", "Downloading", 100_000, 10); // healthy
        input.task_diagnostics = vec![t1, t2, t3];

        let findings = diag.analyze(&input);
        let server = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Server)
            .unwrap();
        assert_eq!(server.related_task_ids.len(), 2);
        assert!(server.related_task_ids.contains(&"t1".to_string()));
        assert!(server.related_task_ids.contains(&"t2".to_string()));
    }

    // ========== Queue not starting edge case ==========

    #[test]
    fn test_queue_not_starting_with_one_queued() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 0;
        input.queued_downloads = 1;

        let findings = diag.analyze(&input);
        let queue = findings
            .iter()
            .find(|f| f.title.contains("Queue Not Starting"))
            .unwrap();
        assert!(queue.description.contains("1 task(s)"));
    }

    // ========== Custom config thresholds ==========

    #[test]
    fn test_custom_slow_threshold() {
        let diag = DownloadDiagnostics::with_config(DiagnosticsConfig {
            slow_download_threshold_bps: 100_000, // 100 KB/s
            ..Default::default()
        });
        let mut input = default_input();
        input.current_speed_bps = 50_000; // 50 KB/s
        input.task_diagnostics = vec![make_task("t1", "Downloading", 50_000, 10)];

        let findings = diag.analyze(&input);
        let slow = findings
            .iter()
            .find(|f| f.title.contains("Slow"))
            .expect("Should detect slow with custom threshold");
        assert!(slow.description.contains("100 KB/s"));
    }

    #[test]
    fn test_custom_stuck_threshold() {
        let diag = DownloadDiagnostics::with_config(DiagnosticsConfig {
            stuck_task_threshold_secs: 600, // 10 minutes
            ..Default::default()
        });
        let mut input = default_input();
        input.task_diagnostics = vec![make_task("t1", "Downloading", 0, 700)]; // 11.6 min

        let findings = diag.analyze(&input);
        let stalled = findings
            .iter()
            .find(|f| f.title.contains("Stalled"))
            .expect("Should detect stalled with custom threshold");
        assert!(stalled.description.contains("10 minutes"));
    }

    #[test]
    fn test_custom_max_retry_threshold() {
        let diag = DownloadDiagnostics::with_config(DiagnosticsConfig {
            max_retry_threshold: 2,
            ..Default::default()
        });
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 100_000, 10);
        task.retry_count = 2;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let retry = findings
            .iter()
            .find(|f| f.title.contains("Retries"))
            .expect("Should flag with custom threshold=2");
        assert!(retry.description.contains("2+ times"));
    }

    #[test]
    fn test_custom_max_consecutive_failures() {
        let diag = DownloadDiagnostics::with_config(DiagnosticsConfig {
            max_consecutive_failures: 1,
            ..Default::default()
        });
        let mut input = default_input();
        let mut task = make_task("t1", "Downloading", 0, 10);
        task.consecutive_failures = 1;
        input.task_diagnostics = vec![task];

        let findings = diag.analyze(&input);
        let server = findings
            .iter()
            .find(|f| f.category == DiagnosticCategory::Server)
            .expect("Should flag with custom threshold=1");
        assert!(server.description.contains("1+ consecutive"));
    }

    // ========== Pretty serde ==========

    #[test]
    fn test_config_pretty_serde() {
        let config = DiagnosticsConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let back: DiagnosticsConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(
            back.slow_download_threshold_bps,
            config.slow_download_threshold_bps
        );
    }

    // ========== All checks disabled ==========

    #[test]
    fn test_all_checks_disabled_except_proxy_and_server() {
        let mut diag = DownloadDiagnostics::new();
        diag.set_config(DiagnosticsConfig {
            check_network: false,
            check_disk: false,
            check_performance: false,
            check_queue: false,
            ..Default::default()
        });
        let mut input = default_input();
        input.network_connected = false;
        input.available_disk_bytes = 0;
        input.proxy_configured = true;
        input.proxy_reachable = Some(false);

        let findings = diag.analyze(&input);
        // Only proxy should be detected (proxy/server checks are always on)
        let network: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Network)
            .collect();
        assert!(network.is_empty());
        let disk: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Disk)
            .collect();
        assert!(disk.is_empty());
        let proxy: Vec<_> = findings
            .iter()
            .filter(|f| f.category == DiagnosticCategory::Proxy)
            .collect();
        assert_eq!(proxy.len(), 1);
    }

    // ========== top_recommendations limit ==========

    #[test]
    fn test_top_recommendations_max_5() {
        let diag = DownloadDiagnostics::new();
        // Create many findings each with unique recommendations
        let findings: Vec<DiagnosticFinding> = (0..10)
            .map(|i| DiagnosticFinding {
                category: DiagnosticCategory::Network,
                severity: DiagnosticSeverity::Critical,
                title: format!("Issue {}", i),
                description: "D".to_string(),
                recommendations: vec![format!("Rec {}", i)],
                related_task_ids: vec![],
            })
            .collect();
        let summary = diag.summarize(&findings);
        assert!(
            summary.top_recommendations.len() <= 5,
            "Top recommendations should be capped at 5"
        );
    }

    // ========== Finding description content ==========

    #[test]
    fn test_low_disk_description_contains_mb_values() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.available_disk_bytes = 500_000_000; // ~476 MB

        let findings = diag.analyze(&input);
        let disk = findings
            .iter()
            .find(|f| f.title.contains("Low Disk"))
            .unwrap();
        // Should contain actual MB and minimum MB
        assert!(disk.description.contains("MB available"));
        assert!(disk.description.contains("minimum:"));
    }

    #[test]
    fn test_high_failure_rate_description_contains_percentage() {
        let diag = DownloadDiagnostics::new();
        let mut input = default_input();
        input.active_downloads = 2;
        input.queued_downloads = 1;
        input.failed_downloads = 5; // 5/8 = 62.5%

        let findings = diag.analyze(&input);
        let failure = findings
            .iter()
            .find(|f| f.title.contains("Failure Rate"))
            .unwrap();
        assert!(failure.description.contains("62%") || failure.description.contains("63%"));
    }
}
