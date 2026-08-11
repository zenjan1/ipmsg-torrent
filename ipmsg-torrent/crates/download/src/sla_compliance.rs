//! Download SLA (Service Level Agreement) Compliance System
//!
//! Tracks whether downloads meet user-defined Service Level Agreements:
//! - Completion time SLA: tasks must finish within a deadline
//! - Minimum speed SLA: tasks must maintain a minimum average speed
//! - Success rate SLA: overall success rate must meet a target percentage
//! - Per-task and aggregate compliance tracking
//! - Compliance scoring and reporting

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

/// Default maximum number of SLA definitions
const DEFAULT_MAX_SLAS: usize = 50;

/// Default maximum compliance history entries per SLA
const DEFAULT_MAX_HISTORY: usize = 200;

/// Types of SLA targets
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlaTarget {
    /// Task must complete within N seconds of creation
    CompletionTimeSecs {
        /// Maximum allowed seconds from creation to completion
        max_secs: u64,
    },
    /// Task must maintain a minimum average download speed
    MinAverageSpeed {
        /// Minimum average speed in bytes per second
        min_bps: u64,
    },
    /// Overall success rate must meet target (evaluated over a window)
    SuccessRate {
        /// Target success rate as percentage (0.0 - 100.0)
        target_percent: f64,
        /// Evaluation window in seconds (e.g., 86400 = last 24h)
        window_secs: u64,
    },
    /// Task must not exceed a maximum number of retries
    MaxRetries {
        /// Maximum allowed retry attempts
        max_retries: u32,
    },
    /// Combined SLA: all sub-targets must be met
    Combined {
        /// List of sub-targets (all must pass)
        targets: Vec<SlaTarget>,
    },
}

impl std::fmt::Display for SlaTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlaTarget::CompletionTimeSecs { max_secs } => {
                write!(f, "complete within {}s", max_secs)
            }
            SlaTarget::MinAverageSpeed { min_bps } => {
                write!(f, "avg speed >= {} B/s", min_bps)
            }
            SlaTarget::SuccessRate {
                target_percent,
                window_secs,
            } => {
                write!(
                    f,
                    "success rate >= {}% over {}s",
                    target_percent, window_secs
                )
            }
            SlaTarget::MaxRetries { max_retries } => {
                write!(f, "max {} retries", max_retries)
            }
            SlaTarget::Combined { targets } => {
                let parts: Vec<String> = targets.iter().map(|t| t.to_string()).collect();
                write!(f, "all of [{}]", parts.join(", "))
            }
        }
    }
}

/// Compliance status for a single evaluation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceStatus {
    /// SLA target is met
    Compliant,
    /// SLA target is violated
    NonCompliant,
    /// Not enough data to evaluate yet
    Pending,
    /// SLA was waived or disabled
    Waived,
}

impl std::fmt::Display for ComplianceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComplianceStatus::Compliant => write!(f, "compliant"),
            ComplianceStatus::NonCompliant => write!(f, "non-compliant"),
            ComplianceStatus::Pending => write!(f, "pending"),
            ComplianceStatus::Waived => write!(f, "waived"),
        }
    }
}

/// Result of evaluating an SLA against a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaEvaluation {
    /// ID of the SLA definition
    pub sla_id: String,
    /// Name of the SLA
    pub sla_name: String,
    /// Task ID being evaluated
    pub task_id: String,
    /// Current compliance status
    pub status: ComplianceStatus,
    /// Compliance score (0.0 - 100.0, higher is better)
    pub score: f64,
    /// Details about the evaluation
    pub details: String,
    /// When this evaluation was performed
    pub evaluated_at: DateTime<Utc>,
}

/// A single compliance history entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceEntry {
    /// Task ID that was evaluated
    pub task_id: String,
    /// Task name for human readability
    pub task_name: String,
    /// Compliance status
    pub status: ComplianceStatus,
    /// Compliance score at time of evaluation
    pub score: f64,
    /// Brief description of why
    pub reason: String,
    /// When this entry was recorded
    pub recorded_at: DateTime<Utc>,
}

/// Definition of an SLA
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaDefinition {
    /// Unique SLA identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Optional description
    pub description: String,
    /// The target to evaluate
    pub target: SlaTarget,
    /// Whether this SLA is enabled
    pub enabled: bool,
    /// Optional tag filter: only evaluate tasks with this tag
    pub tag_filter: Option<String>,
    /// Optional group filter: only evaluate tasks in this group
    pub group_filter: Option<String>,
    /// When this SLA was created
    pub created_at: DateTime<Utc>,
    /// Maximum history entries to keep for this SLA
    #[serde(default = "default_max_history")]
    pub max_history: usize,
}

fn default_max_history() -> usize {
    DEFAULT_MAX_HISTORY
}

/// Summary of SLA compliance across all definitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaSummary {
    /// Total number of SLA definitions
    pub total_slas: usize,
    /// Number of enabled SLAs
    pub enabled_slas: usize,
    /// Per-SLA compliance summary
    pub per_sla: Vec<SlaComplianceSummary>,
    /// Overall compliance score (0.0 - 100.0)
    pub overall_score: f64,
    /// Overall status
    pub overall_status: ComplianceStatus,
    /// Total compliance history entries across all SLAs
    pub total_history_entries: usize,
}

/// Compliance summary for a single SLA definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaComplianceSummary {
    /// SLA definition ID
    pub sla_id: String,
    /// SLA name
    pub sla_name: String,
    /// SLA target description
    pub target: String,
    /// Whether enabled
    pub enabled: bool,
    /// Number of tasks evaluated
    pub tasks_evaluated: usize,
    /// Number currently compliant
    pub tasks_compliant: usize,
    /// Number currently non-compliant
    pub tasks_non_compliant: usize,
    /// Number pending evaluation
    pub tasks_pending: usize,
    /// Compliance rate (compliant / evaluated * 100)
    pub compliance_rate: f64,
    /// Recent history entries count
    pub history_count: usize,
}

/// Configuration for the SLA compliance system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaConfig {
    /// Whether the SLA system is globally enabled
    pub enabled: bool,
    /// Maximum number of SLA definitions
    #[serde(default = "default_max_slas")]
    pub max_slas: usize,
    /// Default max history entries per SLA
    #[serde(default = "default_max_history")]
    pub default_max_history: usize,
    /// Auto-evaluate interval in seconds (0 = manual only)
    #[serde(default = "default_auto_eval_interval")]
    pub auto_eval_interval_secs: u64,
    /// Whether to log compliance violations to activity log
    #[serde(default = "default_true")]
    pub log_violations: bool,
}

fn default_max_slas() -> usize {
    DEFAULT_MAX_SLAS
}

fn default_auto_eval_interval() -> u64 {
    300 // 5 minutes
}

fn default_true() -> bool {
    true
}

impl Default for SlaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_slas: DEFAULT_MAX_SLAS,
            default_max_history: DEFAULT_MAX_HISTORY,
            auto_eval_interval_secs: default_auto_eval_interval(),
            log_violations: true,
        }
    }
}

/// Input data for evaluating a task against an SLA
#[derive(Debug, Clone)]
pub struct TaskSlaData {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Task tags
    pub tags: Vec<String>,
    /// Task group
    pub group: Option<String>,
    /// Whether the task is complete
    pub is_complete: bool,
    /// Whether the task failed
    pub is_failed: bool,
    /// Time the task was created
    pub created_at: DateTime<Utc>,
    /// Time the task completed (if applicable)
    pub completed_at: Option<DateTime<Utc>>,
    /// Current average download speed in B/s
    pub avg_speed_bps: f64,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Current download progress (0.0 - 1.0)
    pub progress: f64,
}

/// Manager for SLA compliance tracking
#[derive(Debug)]
pub struct SlaComplianceManager {
    config: SlaConfig,
    definitions: Vec<SlaDefinition>,
    /// sla_id -> compliance history
    history: HashMap<String, Vec<ComplianceEntry>>,
    data_dir: PathBuf,
}

impl SlaComplianceManager {
    /// Create a new SLA compliance manager
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            config: SlaConfig::default(),
            definitions: Vec::new(),
            history: HashMap::new(),
            data_dir,
        }
    }

    /// Load configuration from disk
    pub async fn load_config(&mut self) -> std::io::Result<()> {
        let path = self.data_dir.join("sla_compliance_config.json");
        match fs::read_to_string(&path).await {
            Ok(content) => match serde_json::from_str(&content) {
                Ok(config) => {
                    self.config = config;
                    Ok(())
                }
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse SLA config: {e}"),
                )),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Save configuration to disk
    pub async fn save_config(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("sla_compliance_config.json");
        let content = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        fs::write(&path, content).await
    }

    /// Load SLA definitions from disk
    pub async fn load_definitions(&mut self) -> std::io::Result<()> {
        let path = self.data_dir.join("sla_definitions.json");
        match fs::read_to_string(&path).await {
            Ok(content) => match serde_json::from_str(&content) {
                Ok(defs) => {
                    self.definitions = defs;
                    Ok(())
                }
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse SLA definitions: {e}"),
                )),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Save SLA definitions to disk
    pub async fn save_definitions(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("sla_definitions.json");
        let content =
            serde_json::to_string_pretty(&self.definitions).map_err(std::io::Error::other)?;
        fs::write(&path, content).await
    }

    /// Load compliance history from disk
    pub async fn load_history(&mut self) -> std::io::Result<()> {
        let path = self.data_dir.join("sla_compliance_history.json");
        match fs::read_to_string(&path).await {
            Ok(content) => match serde_json::from_str(&content) {
                Ok(hist) => {
                    self.history = hist;
                    Ok(())
                }
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse SLA history: {e}"),
                )),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Save compliance history to disk
    pub async fn save_history(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("sla_compliance_history.json");
        let content = serde_json::to_string_pretty(&self.history).map_err(std::io::Error::other)?;
        fs::write(&path, content).await
    }

    /// Get current configuration
    pub fn get_config(&self) -> &SlaConfig {
        &self.config
    }

    /// Update configuration
    pub async fn set_config(&mut self, config: SlaConfig) -> std::io::Result<()> {
        self.config = config;
        self.save_config().await
    }

    /// Add a new SLA definition
    pub async fn add_sla(&mut self, mut def: SlaDefinition) -> std::io::Result<String> {
        if self.definitions.len() >= self.config.max_slas {
            return Err(std::io::Error::other(format!(
                "maximum number of SLA definitions reached ({})",
                self.config.max_slas
            )));
        }
        if def.id.is_empty() {
            def.id = format!("sla-{}", chrono::Utc::now().timestamp_millis());
        }
        let id = def.id.clone();
        self.definitions.push(def);
        self.save_definitions().await?;
        Ok(id)
    }

    /// Remove an SLA definition
    pub async fn remove_sla(&mut self, sla_id: &str) -> std::io::Result<bool> {
        let before = self.definitions.len();
        self.definitions.retain(|d| d.id != sla_id);
        self.history.remove(sla_id);
        if self.definitions.len() < before {
            self.save_definitions().await?;
            self.save_history().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get an SLA definition by ID
    pub fn get_sla(&self, sla_id: &str) -> Option<&SlaDefinition> {
        self.definitions.iter().find(|d| d.id == sla_id)
    }

    /// List all SLA definitions
    pub fn list_slas(&self) -> &[SlaDefinition] {
        &self.definitions
    }

    /// Enable or disable an SLA
    pub async fn set_sla_enabled(&mut self, sla_id: &str, enabled: bool) -> std::io::Result<bool> {
        if let Some(def) = self.definitions.iter_mut().find(|d| d.id == sla_id) {
            def.enabled = enabled;
            self.save_definitions().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Evaluate a single task against a single SLA target
    pub fn evaluate_task(&self, task: &TaskSlaData, sla: &SlaDefinition) -> SlaEvaluation {
        let (status, score, details) = self.evaluate_target(task, &sla.target);

        SlaEvaluation {
            sla_id: sla.id.clone(),
            sla_name: sla.name.clone(),
            task_id: task.task_id.clone(),
            status,
            score,
            details,
            evaluated_at: Utc::now(),
        }
    }

    /// Evaluate a task against an SLA target, returning (status, score, details)
    fn evaluate_target(
        &self,
        task: &TaskSlaData,
        target: &SlaTarget,
    ) -> (ComplianceStatus, f64, String) {
        match target {
            SlaTarget::CompletionTimeSecs { max_secs } => {
                if task.is_complete {
                    if let Some(completed_at) = task.completed_at {
                        let elapsed = (completed_at - task.created_at).num_seconds().max(0) as u64;
                        if elapsed <= *max_secs {
                            let margin = (*max_secs as f64 - elapsed as f64) / *max_secs as f64;
                            let score = 50.0 + margin * 50.0;
                            (
                                ComplianceStatus::Compliant,
                                score,
                                format!(
                                    "completed in {}s (limit: {}s, margin: {:.0}%)",
                                    elapsed,
                                    max_secs,
                                    margin * 100.0
                                ),
                            )
                        } else {
                            let overage = elapsed - max_secs;
                            let score = (50.0 * (1.0 - overage as f64 / *max_secs as f64)).max(0.0);
                            (
                                ComplianceStatus::NonCompliant,
                                score,
                                format!(
                                    "completed in {}s (limit: {}s, over by {}s)",
                                    elapsed, max_secs, overage
                                ),
                            )
                        }
                    } else {
                        (
                            ComplianceStatus::Pending,
                            50.0,
                            "completed but no completion timestamp".to_string(),
                        )
                    }
                } else if task.is_failed {
                    (
                        ComplianceStatus::NonCompliant,
                        0.0,
                        "task failed before completing within SLA time".to_string(),
                    )
                } else {
                    // Task still in progress - check if we're still within time
                    let elapsed = (Utc::now() - task.created_at).num_seconds().max(0) as u64;
                    if elapsed <= *max_secs {
                        let remaining = *max_secs - elapsed;
                        let progress_pct = task.progress * 100.0;
                        (
                            ComplianceStatus::Pending,
                            50.0 + (remaining as f64 / *max_secs as f64) * 25.0,
                            format!(
                                "in progress: {}s elapsed, {}s remaining, {:.1}% done",
                                elapsed, remaining, progress_pct
                            ),
                        )
                    } else {
                        (
                            ComplianceStatus::NonCompliant,
                            0.0,
                            format!(
                                "exceeded time limit: {}s elapsed > {}s limit",
                                elapsed, max_secs
                            ),
                        )
                    }
                }
            }
            SlaTarget::MinAverageSpeed { min_bps } => {
                if task.is_complete || task.progress > 0.0 {
                    let speed = task.avg_speed_bps;
                    if speed >= *min_bps as f64 {
                        let ratio = speed / *min_bps as f64;
                        let score = (50.0 + (ratio.min(3.0) / 3.0) * 50.0).min(100.0);
                        (
                            ComplianceStatus::Compliant,
                            score,
                            format!("avg speed {:.0} B/s >= {} B/s target", speed, min_bps),
                        )
                    } else {
                        let ratio = speed / *min_bps as f64;
                        let score = ratio * 50.0;
                        (
                            ComplianceStatus::NonCompliant,
                            score,
                            format!(
                                "avg speed {:.0} B/s < {} B/s target ({:.0}% of target)",
                                speed,
                                min_bps,
                                ratio * 100.0
                            ),
                        )
                    }
                } else {
                    (
                        ComplianceStatus::Pending,
                        50.0,
                        "no speed data available yet".to_string(),
                    )
                }
            }
            SlaTarget::SuccessRate {
                target_percent: _,
                window_secs: _,
            } => {
                // Success rate is evaluated at aggregate level, not per-task
                // For per-task evaluation, we check if the task itself succeeded
                if task.is_complete {
                    (
                        ComplianceStatus::Compliant,
                        100.0,
                        "task completed successfully".to_string(),
                    )
                } else if task.is_failed {
                    (
                        ComplianceStatus::NonCompliant,
                        0.0,
                        "task failed".to_string(),
                    )
                } else {
                    (
                        ComplianceStatus::Pending,
                        50.0,
                        "task still in progress".to_string(),
                    )
                }
            }
            SlaTarget::MaxRetries { max_retries } => {
                if task.retry_count <= *max_retries {
                    let ratio = task.retry_count as f64 / *max_retries as f64;
                    let score = 100.0 - ratio * 50.0;
                    (
                        ComplianceStatus::Compliant,
                        score,
                        format!("{} retries <= {} max", task.retry_count, max_retries),
                    )
                } else {
                    let overage = task.retry_count - max_retries;
                    let score =
                        (50.0 * (1.0 - overage as f64 / (*max_retries).max(1) as f64)).max(0.0);
                    (
                        ComplianceStatus::NonCompliant,
                        score,
                        format!(
                            "{} retries > {} max (over by {})",
                            task.retry_count, max_retries, overage
                        ),
                    )
                }
            }
            SlaTarget::Combined { targets } => {
                let mut all_compliant = true;
                let mut any_non_compliant = false;
                let mut total_score = 0.0;
                let mut details_parts = Vec::new();

                for sub_target in targets {
                    let (status, score, detail) = self.evaluate_target(task, sub_target);
                    total_score += score;
                    details_parts.push(detail);

                    match status {
                        ComplianceStatus::NonCompliant => {
                            all_compliant = false;
                            any_non_compliant = true;
                        }
                        ComplianceStatus::Pending => {
                            all_compliant = false;
                        }
                        _ => {}
                    }
                }

                let avg_score = if targets.is_empty() {
                    50.0
                } else {
                    total_score / targets.len() as f64
                };

                if all_compliant {
                    (
                        ComplianceStatus::Compliant,
                        avg_score,
                        details_parts.join("; "),
                    )
                } else if any_non_compliant {
                    (
                        ComplianceStatus::NonCompliant,
                        avg_score,
                        details_parts.join("; "),
                    )
                } else {
                    (
                        ComplianceStatus::Pending,
                        avg_score,
                        details_parts.join("; "),
                    )
                }
            }
        }
    }

    /// Evaluate all enabled SLAs against a set of tasks, recording results
    pub async fn evaluate_all(
        &mut self,
        tasks: &[TaskSlaData],
    ) -> std::io::Result<Vec<SlaEvaluation>> {
        let mut evaluations = Vec::new();
        let enabled_slas: Vec<SlaDefinition> = self
            .definitions
            .iter()
            .filter(|d| d.enabled)
            .cloned()
            .collect();

        for sla in &enabled_slas {
            for task in tasks {
                // Apply filters
                if sla
                    .tag_filter
                    .as_ref()
                    .is_some_and(|tf| !task.tags.contains(tf))
                {
                    continue;
                }
                if sla
                    .group_filter
                    .as_ref()
                    .is_some_and(|gf| task.group.as_deref() != Some(gf.as_str()))
                {
                    continue;
                }

                let eval = self.evaluate_task(task, sla);

                // Record in history
                let entry = ComplianceEntry {
                    task_id: task.task_id.clone(),
                    task_name: task.task_name.clone(),
                    status: eval.status,
                    score: eval.score,
                    reason: eval.details.clone(),
                    recorded_at: eval.evaluated_at,
                };

                let history = self.history.entry(sla.id.clone()).or_default();
                history.push(entry);

                // Trim history
                let max_hist = sla.max_history;
                if history.len() > max_hist {
                    let drain_count = history.len() - max_hist;
                    history.drain(..drain_count);
                }

                evaluations.push(eval);
            }
        }

        self.save_history().await?;
        Ok(evaluations)
    }

    /// Get compliance summary for all SLAs
    pub fn get_summary(&self) -> SlaSummary {
        let mut per_sla = Vec::new();
        let mut total_history = 0;
        let mut total_score = 0.0;
        let mut scored_count = 0;
        let mut any_non_compliant = false;
        let mut any_pending = false;

        for sla in &self.definitions {
            let history = self.history.get(&sla.id);
            let hist_entries = history.map(|h| h.len()).unwrap_or(0);
            total_history += hist_entries;

            // Count current compliance by looking at latest entry per task
            let mut task_latest: HashMap<String, &ComplianceEntry> = HashMap::new();
            if let Some(entries) = history {
                for entry in entries {
                    task_latest.insert(entry.task_id.clone(), entry);
                }
            }

            let tasks_evaluated = task_latest.len();
            let tasks_compliant = task_latest
                .values()
                .filter(|e| e.status == ComplianceStatus::Compliant)
                .count();
            let tasks_non_compliant = task_latest
                .values()
                .filter(|e| e.status == ComplianceStatus::NonCompliant)
                .count();
            let tasks_pending = task_latest
                .values()
                .filter(|e| e.status == ComplianceStatus::Pending)
                .count();

            let compliance_rate = if tasks_evaluated > 0 {
                tasks_compliant as f64 / tasks_evaluated as f64 * 100.0
            } else {
                if sla.enabled {
                    any_pending = true;
                }
                0.0
            };

            if tasks_non_compliant > 0 {
                any_non_compliant = true;
            }
            if tasks_pending > 0 {
                any_pending = true;
            }

            if compliance_rate > 0.0 {
                total_score += compliance_rate;
                scored_count += 1;
            }

            per_sla.push(SlaComplianceSummary {
                sla_id: sla.id.clone(),
                sla_name: sla.name.clone(),
                target: sla.target.to_string(),
                enabled: sla.enabled,
                tasks_evaluated,
                tasks_compliant,
                tasks_non_compliant,
                tasks_pending,
                compliance_rate,
                history_count: hist_entries,
            });
        }

        let overall_score = if scored_count > 0 {
            total_score / scored_count as f64
        } else {
            0.0
        };

        let overall_status = if any_non_compliant {
            ComplianceStatus::NonCompliant
        } else if any_pending {
            ComplianceStatus::Pending
        } else if scored_count > 0 {
            ComplianceStatus::Compliant
        } else {
            ComplianceStatus::Pending
        };

        SlaSummary {
            total_slas: self.definitions.len(),
            enabled_slas: self.definitions.iter().filter(|d| d.enabled).count(),
            per_sla,
            overall_score,
            overall_status,
            total_history_entries: total_history,
        }
    }

    /// Get compliance history for a specific SLA
    pub fn get_history(&self, sla_id: &str) -> Option<&Vec<ComplianceEntry>> {
        self.history.get(sla_id)
    }

    /// Clear compliance history for a specific SLA
    pub async fn clear_history(&mut self, sla_id: &str) -> std::io::Result<bool> {
        if self.history.remove(sla_id).is_some() {
            self.save_history().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Clear all compliance history
    pub async fn clear_all_history(&mut self) -> std::io::Result<()> {
        self.history.clear();
        self.save_history().await
    }

    /// Format a human-readable compliance report
    pub fn format_report(&self, summary: &SlaSummary) -> String {
        let mut out = String::new();

        out.push_str("=== SLA Compliance Report ===\n\n");

        let status_emoji = match summary.overall_status {
            ComplianceStatus::Compliant => "✅",
            ComplianceStatus::NonCompliant => "❌",
            ComplianceStatus::Pending => "⏳",
            ComplianceStatus::Waived => "⚪",
        };

        out.push_str(&format!(
            "Overall: {} {} ({:.1}% score)\n",
            status_emoji, summary.overall_status, summary.overall_score
        ));
        out.push_str(&format!(
            "SLAs: {} total, {} enabled\n",
            summary.total_slas, summary.enabled_slas
        ));
        out.push_str(&format!(
            "History: {} entries\n\n",
            summary.total_history_entries
        ));

        if summary.per_sla.is_empty() {
            out.push_str("No SLA definitions configured.\n");
            out.push_str("Use the CLI or REST API to add SLA targets.\n");
            return out;
        }

        for sla_summary in &summary.per_sla {
            let enabled_str = if sla_summary.enabled { "🟢" } else { "⚪" };
            out.push_str(&format!(
                "{} [{}] {} - {}\n",
                enabled_str, sla_summary.sla_id, sla_summary.sla_name, sla_summary.target
            ));

            if !sla_summary.enabled {
                out.push_str("   (disabled)\n");
                continue;
            }

            out.push_str(&format!(
                "   Tasks: {} evaluated, {} ✅, {} ❌, {} ⏳\n",
                sla_summary.tasks_evaluated,
                sla_summary.tasks_compliant,
                sla_summary.tasks_non_compliant,
                sla_summary.tasks_pending
            ));
            out.push_str(&format!(
                "   Compliance rate: {:.1}%\n",
                sla_summary.compliance_rate
            ));
            out.push_str(&format!(
                "   History: {} entries\n",
                sla_summary.history_count
            ));
            out.push('\n');
        }

        out
    }
}

/// Load all SLA data from disk
pub async fn load_sla_data(
    data_dir: &Path,
) -> std::io::Result<(
    SlaConfig,
    Vec<SlaDefinition>,
    HashMap<String, Vec<ComplianceEntry>>,
)> {
    let config_path = data_dir.join("sla_compliance_config.json");
    let config = match fs::read_to_string(&config_path).await {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => SlaConfig::default(),
    };

    let defs_path = data_dir.join("sla_definitions.json");
    let definitions = match fs::read_to_string(&defs_path).await {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => Vec::new(),
    };

    let hist_path = data_dir.join("sla_compliance_history.json");
    let history = match fs::read_to_string(&hist_path).await {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => HashMap::new(),
    };

    Ok((config, definitions, history))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_task(id: &str, name: &str) -> TaskSlaData {
        TaskSlaData {
            task_id: id.to_string(),
            task_name: name.to_string(),
            tags: vec![],
            group: None,
            is_complete: false,
            is_failed: false,
            created_at: Utc::now() - chrono::Duration::seconds(60),
            completed_at: None,
            avg_speed_bps: 0.0,
            retry_count: 0,
            progress: 0.0,
        }
    }

    fn make_completed_task(id: &str, elapsed_secs: i64, avg_speed: f64) -> TaskSlaData {
        let created = Utc::now() - chrono::Duration::seconds(elapsed_secs);
        TaskSlaData {
            task_id: id.to_string(),
            task_name: format!("task-{id}"),
            tags: vec![],
            group: None,
            is_complete: true,
            is_failed: false,
            created_at: created,
            completed_at: Some(created + chrono::Duration::seconds(elapsed_secs)),
            avg_speed_bps: avg_speed,
            retry_count: 0,
            progress: 1.0,
        }
    }

    #[test]
    fn test_completion_time_compliant() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let task = make_completed_task("t1", 100, 1000.0);
        let sla = SlaDefinition {
            id: "sla1".to_string(),
            name: "Fast completion".to_string(),
            description: String::new(),
            target: SlaTarget::CompletionTimeSecs { max_secs: 300 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Compliant);
        assert!(eval.score >= 50.0);
    }

    #[test]
    fn test_completion_time_non_compliant() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let task = make_completed_task("t1", 500, 1000.0);
        let sla = SlaDefinition {
            id: "sla1".to_string(),
            name: "Fast completion".to_string(),
            description: String::new(),
            target: SlaTarget::CompletionTimeSecs { max_secs: 300 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::NonCompliant);
        assert!(eval.score < 50.0);
    }

    #[test]
    fn test_min_speed_compliant() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let task = make_completed_task("t1", 100, 2_000_000.0);
        let sla = SlaDefinition {
            id: "sla2".to_string(),
            name: "Min speed".to_string(),
            description: String::new(),
            target: SlaTarget::MinAverageSpeed { min_bps: 1_000_000 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Compliant);
        assert!(eval.score > 50.0);
    }

    #[test]
    fn test_min_speed_non_compliant() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let task = make_completed_task("t1", 100, 500_000.0);
        let sla = SlaDefinition {
            id: "sla2".to_string(),
            name: "Min speed".to_string(),
            description: String::new(),
            target: SlaTarget::MinAverageSpeed { min_bps: 1_000_000 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::NonCompliant);
        assert!(eval.score < 50.0);
    }

    #[test]
    fn test_max_retries_compliant() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let mut task = make_task("t1", "test");
        task.retry_count = 2;
        task.is_complete = true;
        task.progress = 1.0;

        let sla = SlaDefinition {
            id: "sla3".to_string(),
            name: "Max retries".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 5 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Compliant);
        assert!(eval.score > 50.0);
    }

    #[test]
    fn test_max_retries_non_compliant() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let mut task = make_task("t1", "test");
        task.retry_count = 10;
        task.is_complete = true;
        task.progress = 1.0;

        let sla = SlaDefinition {
            id: "sla3".to_string(),
            name: "Max retries".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 5 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::NonCompliant);
    }

    #[test]
    fn test_combined_sla() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let task = make_completed_task("t1", 100, 2_000_000.0);

        let sla = SlaDefinition {
            id: "sla4".to_string(),
            name: "Combined".to_string(),
            description: String::new(),
            target: SlaTarget::Combined {
                targets: vec![
                    SlaTarget::CompletionTimeSecs { max_secs: 300 },
                    SlaTarget::MinAverageSpeed { min_bps: 1_000_000 },
                ],
            },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Compliant);
    }

    #[test]
    fn test_combined_sla_partial_fail() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        // Fast but slow speed
        let task = make_completed_task("t1", 100, 500_000.0);

        let sla = SlaDefinition {
            id: "sla4".to_string(),
            name: "Combined".to_string(),
            description: String::new(),
            target: SlaTarget::Combined {
                targets: vec![
                    SlaTarget::CompletionTimeSecs { max_secs: 300 },
                    SlaTarget::MinAverageSpeed { min_bps: 1_000_000 },
                ],
            },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::NonCompliant);
    }

    #[test]
    fn test_pending_evaluation() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let task = make_task("t1", "test"); // not complete, not failed

        let sla = SlaDefinition {
            id: "sla1".to_string(),
            name: "Speed".to_string(),
            description: String::new(),
            target: SlaTarget::MinAverageSpeed { min_bps: 1_000_000 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Pending);
    }

    #[tokio::test]
    async fn test_add_and_remove_sla() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "test-sla".to_string(),
            name: "Test SLA".to_string(),
            description: "A test".to_string(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let id = mgr.add_sla(def).await.unwrap();
        assert_eq!(id, "test-sla");
        assert_eq!(mgr.list_slas().len(), 1);

        let removed = mgr.remove_sla("test-sla").await.unwrap();
        assert!(removed);
        assert_eq!(mgr.list_slas().len(), 0);
    }

    #[tokio::test]
    async fn test_config_persistence() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let mut config = SlaConfig::default();
        config.max_slas = 10;
        config.auto_eval_interval_secs = 600;

        mgr.set_config(config).await.unwrap();

        let mut mgr2 = SlaComplianceManager::new(tmp.path().to_path_buf());
        mgr2.load_config().await.unwrap();
        assert_eq!(mgr2.get_config().max_slas, 10);
        assert_eq!(mgr2.get_config().auto_eval_interval_secs, 600);
    }

    #[tokio::test]
    async fn test_definitions_persistence() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "persist-sla".to_string(),
            name: "Persist Test".to_string(),
            description: String::new(),
            target: SlaTarget::CompletionTimeSecs { max_secs: 3600 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();

        let mut mgr2 = SlaComplianceManager::new(tmp.path().to_path_buf());
        mgr2.load_definitions().await.unwrap();
        assert_eq!(mgr2.list_slas().len(), 1);
        assert_eq!(mgr2.list_slas()[0].id, "persist-sla");
    }

    #[tokio::test]
    async fn test_evaluate_all() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "eval-sla".to_string(),
            name: "Speed check".to_string(),
            description: String::new(),
            target: SlaTarget::MinAverageSpeed { min_bps: 1_000_000 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();

        let tasks = vec![
            make_completed_task("t1", 100, 2_000_000.0),
            make_completed_task("t2", 100, 500_000.0),
        ];

        let evals = mgr.evaluate_all(&tasks).await.unwrap();
        assert_eq!(evals.len(), 2);
        assert_eq!(evals[0].status, ComplianceStatus::Compliant);
        assert_eq!(evals[1].status, ComplianceStatus::NonCompliant);

        // Check history was recorded
        let history = mgr.get_history("eval-sla").unwrap();
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn test_tag_filter() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "tag-sla".to_string(),
            name: "Tagged only".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: Some("important".to_string()),
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();

        let mut task1 = make_completed_task("t1", 100, 1000.0);
        task1.tags = vec!["important".to_string()];

        let task2 = make_completed_task("t2", 100, 1000.0);
        // task2 has no tags, should be skipped

        let evals = mgr.evaluate_all(&[task1, task2]).await.unwrap();
        assert_eq!(evals.len(), 1);
        assert_eq!(evals[0].task_id, "t1");
    }

    #[tokio::test]
    async fn test_summary() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "sum-sla".to_string(),
            name: "Summary test".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();

        let tasks = vec![
            make_completed_task("t1", 100, 1000.0),
            make_completed_task("t2", 100, 1000.0),
        ];

        mgr.evaluate_all(&tasks).await.unwrap();

        let summary = mgr.get_summary();
        assert_eq!(summary.total_slas, 1);
        assert_eq!(summary.enabled_slas, 1);
        assert_eq!(summary.per_sla.len(), 1);
        assert_eq!(summary.per_sla[0].tasks_evaluated, 2);
        assert_eq!(summary.per_sla[0].tasks_compliant, 2);
    }

    #[tokio::test]
    async fn test_clear_history() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "clear-sla".to_string(),
            name: "Clear test".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();
        let tasks = vec![make_completed_task("t1", 100, 1000.0)];
        mgr.evaluate_all(&tasks).await.unwrap();

        assert!(mgr.get_history("clear-sla").is_some());

        let cleared = mgr.clear_history("clear-sla").await.unwrap();
        assert!(cleared);
        assert!(mgr.get_history("clear-sla").is_none());
    }

    #[test]
    fn test_format_report_empty() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let summary = mgr.get_summary();
        let report = mgr.format_report(&summary);
        assert!(report.contains("No SLA definitions configured"));
    }

    #[test]
    fn test_format_report_with_data() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let summary = SlaSummary {
            total_slas: 2,
            enabled_slas: 1,
            per_sla: vec![
                SlaComplianceSummary {
                    sla_id: "s1".to_string(),
                    sla_name: "Speed SLA".to_string(),
                    target: "avg speed >= 1000000 B/s".to_string(),
                    enabled: true,
                    tasks_evaluated: 10,
                    tasks_compliant: 8,
                    tasks_non_compliant: 2,
                    tasks_pending: 0,
                    compliance_rate: 80.0,
                    history_count: 50,
                },
                SlaComplianceSummary {
                    sla_id: "s2".to_string(),
                    sla_name: "Disabled SLA".to_string(),
                    target: "complete within 3600s".to_string(),
                    enabled: false,
                    tasks_evaluated: 0,
                    tasks_compliant: 0,
                    tasks_non_compliant: 0,
                    tasks_pending: 0,
                    compliance_rate: 0.0,
                    history_count: 0,
                },
            ],
            overall_score: 80.0,
            overall_status: ComplianceStatus::NonCompliant,
            total_history_entries: 50,
        };

        let report = mgr.format_report(&summary);
        assert!(report.contains("SLA Compliance Report"));
        assert!(report.contains("Speed SLA"));
        assert!(report.contains("80.0%"));
        assert!(report.contains("(disabled)"));
    }

    #[test]
    fn test_sla_target_display() {
        let t1 = SlaTarget::CompletionTimeSecs { max_secs: 3600 };
        assert_eq!(t1.to_string(), "complete within 3600s");

        let t2 = SlaTarget::MinAverageSpeed { min_bps: 1_000_000 };
        assert_eq!(t2.to_string(), "avg speed >= 1000000 B/s");

        let t3 = SlaTarget::SuccessRate {
            target_percent: 95.0,
            window_secs: 86400,
        };
        assert_eq!(t3.to_string(), "success rate >= 95% over 86400s");

        let t4 = SlaTarget::MaxRetries { max_retries: 5 };
        assert_eq!(t4.to_string(), "max 5 retries");
    }

    #[test]
    fn test_compliance_status_display() {
        assert_eq!(ComplianceStatus::Compliant.to_string(), "compliant");
        assert_eq!(ComplianceStatus::NonCompliant.to_string(), "non-compliant");
        assert_eq!(ComplianceStatus::Pending.to_string(), "pending");
        assert_eq!(ComplianceStatus::Waived.to_string(), "waived");
    }

    #[test]
    fn test_default_config() {
        let config = SlaConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_slas, DEFAULT_MAX_SLAS);
        assert_eq!(config.default_max_history, DEFAULT_MAX_HISTORY);
        assert_eq!(config.auto_eval_interval_secs, 300);
        assert!(config.log_violations);
    }

    #[tokio::test]
    async fn test_set_sla_enabled() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "toggle-sla".to_string(),
            name: "Toggle".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();
        assert!(mgr.get_sla("toggle-sla").unwrap().enabled);

        mgr.set_sla_enabled("toggle-sla", false).await.unwrap();
        assert!(!mgr.get_sla("toggle-sla").unwrap().enabled);

        let result = mgr.set_sla_enabled("nonexistent", false).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_max_slas_limit() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());
        mgr.config.max_slas = 2;

        for i in 0..2 {
            let def = SlaDefinition {
                id: format!("sla-{i}"),
                name: format!("SLA {i}"),
                description: String::new(),
                target: SlaTarget::MaxRetries { max_retries: 3 },
                enabled: true,
                tag_filter: None,
                group_filter: None,
                created_at: Utc::now(),
                max_history: DEFAULT_MAX_HISTORY,
            };
            mgr.add_sla(def).await.unwrap();
        }

        let def = SlaDefinition {
            id: "sla-overflow".to_string(),
            name: "Overflow".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };
        let result = mgr.add_sla(def).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_failed_task_completion_time() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let created = Utc::now() - chrono::Duration::seconds(100);
        let task = TaskSlaData {
            task_id: "t1".to_string(),
            task_name: "failed-task".to_string(),
            tags: vec![],
            group: None,
            is_complete: false,
            is_failed: true,
            created_at: created,
            completed_at: None,
            avg_speed_bps: 0.0,
            retry_count: 3,
            progress: 0.5,
        };

        let sla = SlaDefinition {
            id: "sla1".to_string(),
            name: "Completion".to_string(),
            description: String::new(),
            target: SlaTarget::CompletionTimeSecs { max_secs: 300 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::NonCompliant);
        assert_eq!(eval.score, 0.0);
    }

    #[test]
    fn test_group_filter() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let mut task = make_completed_task("t1", 100, 1000.0);
        task.group = Some("videos".to_string());

        let sla = SlaDefinition {
            id: "group-sla".to_string(),
            name: "Group filter".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: Some("videos".to_string()),
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Compliant);

        // Task with wrong group should not match
        let mut task2 = make_completed_task("t2", 100, 1000.0);
        task2.group = Some("music".to_string());

        // The evaluate_task doesn't filter - filtering happens in evaluate_all
        // So evaluate_task should still evaluate it
        let eval2 = mgr.evaluate_task(&task2, &sla);
        assert_eq!(eval2.status, ComplianceStatus::Compliant);
    }

    #[tokio::test]
    async fn test_history_persistence() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SlaComplianceManager::new(tmp.path().to_path_buf());

        let def = SlaDefinition {
            id: "hist-sla".to_string(),
            name: "History".to_string(),
            description: String::new(),
            target: SlaTarget::MaxRetries { max_retries: 3 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        mgr.add_sla(def).await.unwrap();
        let tasks = vec![make_completed_task("t1", 100, 1000.0)];
        mgr.evaluate_all(&tasks).await.unwrap();

        // Reload and check history persists
        let mut mgr2 = SlaComplianceManager::new(tmp.path().to_path_buf());
        mgr2.load_definitions().await.unwrap();
        mgr2.load_history().await.unwrap();

        let history = mgr2.get_history("hist-sla").unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].task_id, "t1");
    }

    #[test]
    fn test_in_progress_task_within_time_limit() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let mut task = make_task("t1", "in-progress");
        task.progress = 0.5;
        task.created_at = Utc::now() - chrono::Duration::seconds(100);

        let sla = SlaDefinition {
            id: "sla1".to_string(),
            name: "Time limit".to_string(),
            description: String::new(),
            target: SlaTarget::CompletionTimeSecs { max_secs: 300 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::Pending);
        assert!(eval.score > 50.0); // Still has time remaining
    }

    #[test]
    fn test_in_progress_task_exceeded_time_limit() {
        let mgr = SlaComplianceManager::new(PathBuf::from("/tmp"));
        let mut task = make_task("t1", "stuck");
        task.progress = 0.1;
        task.created_at = Utc::now() - chrono::Duration::seconds(500);

        let sla = SlaDefinition {
            id: "sla1".to_string(),
            name: "Time limit".to_string(),
            description: String::new(),
            target: SlaTarget::CompletionTimeSecs { max_secs: 300 },
            enabled: true,
            tag_filter: None,
            group_filter: None,
            created_at: Utc::now(),
            max_history: DEFAULT_MAX_HISTORY,
        };

        let eval = mgr.evaluate_task(&task, &sla);
        assert_eq!(eval.status, ComplianceStatus::NonCompliant);
        assert_eq!(eval.score, 0.0);
    }
}
