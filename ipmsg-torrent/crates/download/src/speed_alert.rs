//! Download speed trend alerts
//!
//! Monitors download speed trends and generates alerts when speed degrades
//! or drops below configurable thresholds. Helps users identify problematic
//! downloads early.
//!
//! # Features
//!
//! - Configurable speed threshold alerts (absolute BPS)
//! - Trend-based alerts (sustained speed decline)
//! - Per-task or global monitoring
//! - Alert history with timestamps
//! - Persistent configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Maximum number of alerts to keep in history
const MAX_ALERT_HISTORY: usize = 200;

/// Type of speed alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertType {
    /// Speed dropped below absolute threshold
    BelowThreshold,
    /// Speed has been declining for sustained period
    SustainedDecline,
    /// Speed dropped to near-zero (likely stalled)
    NearStall,
}

impl AlertType {
    pub fn label(&self) -> &'static str {
        match self {
            Self::BelowThreshold => "below_threshold",
            Self::SustainedDecline => "sustained_decline",
            Self::NearStall => "near_stall",
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            Self::BelowThreshold => "⚠️",
            Self::SustainedDecline => "📉",
            Self::NearStall => "🛑",
        }
    }
}

/// Severity of a speed alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl AlertSeverity {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            Self::Info => "ℹ️",
            Self::Warning => "⚠️",
            Self::Critical => "🚨",
        }
    }
}

/// A single speed alert event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedAlert {
    /// Unique alert ID (incrementing counter)
    pub id: u64,
    /// Task ID that triggered the alert
    pub task_id: String,
    /// Task name (for display)
    pub task_name: String,
    /// Type of alert
    pub alert_type: AlertType,
    /// Severity level
    pub severity: AlertSeverity,
    /// Human-readable message
    pub message: String,
    /// Current speed when alert fired (bytes/sec)
    pub current_speed_bps: f64,
    /// Threshold that was crossed (if applicable)
    pub threshold_bps: Option<f64>,
    /// Timestamp (Unix epoch seconds)
    pub timestamp: u64,
}

impl SpeedAlert {
    pub fn format_display(&self) -> String {
        format!(
            "{} {} [{}] {} - {} (current: {}/s)",
            self.severity.emoji(),
            self.alert_type.emoji(),
            self.severity.label(),
            self.task_name,
            self.message,
            format_speed(self.current_speed_bps),
        )
    }
}

fn format_speed(bps: f64) -> String {
    if bps >= 1_000_000.0 {
        format!("{:.1} MB", bps / 1_000_000.0)
    } else if bps >= 1_000.0 {
        format!("{:.1} KB", bps / 1_000.0)
    } else {
        format!("{:.0} B", bps)
    }
}

/// Configuration for speed alert rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedAlertConfig {
    /// Enable/disable all speed alerts
    pub enabled: bool,
    /// Absolute speed threshold: alert if speed drops below this (bytes/sec)
    /// None = disabled
    pub min_speed_bps: Option<f64>,
    /// Number of consecutive samples below threshold before alerting
    pub min_speed_consecutive: u32,
    /// Enable sustained decline detection
    pub decline_detection: bool,
    /// Number of consecutive declining samples to trigger alert
    pub decline_samples: u32,
    /// Minimum speed drop ratio to count as decline (e.g., 0.5 = 50% drop)
    pub decline_ratio: f64,
    /// Near-stall threshold: alert if speed drops below this
    pub near_stall_bps: f64,
    /// Cooldown between alerts for same task (seconds)
    pub alert_cooldown_secs: u64,
}

impl Default for SpeedAlertConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_speed_bps: Some(10_000.0), // 10 KB/s
            min_speed_consecutive: 3,
            decline_detection: true,
            decline_samples: 5,
            decline_ratio: 0.5,
            near_stall_bps: 100.0,
            alert_cooldown_secs: 300, // 5 minutes
        }
    }
}

/// Per-task monitoring state
#[derive(Debug, Clone)]
struct TaskMonitorState {
    /// Recent speed samples (oldest first)
    speed_samples: Vec<f64>,
    /// Consecutive samples below min_speed threshold
    below_threshold_count: u32,
    /// Whether we already alerted for below-threshold
    alerted_below_threshold: bool,
    /// Whether we already alerted for sustained decline
    alerted_decline: bool,
    /// Whether we already alerted for near-stall
    alerted_near_stall: bool,
    /// Last alert timestamp for this task
    last_alert_time: u64,
}

impl TaskMonitorState {
    fn new() -> Self {
        Self {
            speed_samples: Vec::new(),
            below_threshold_count: 0,
            alerted_below_threshold: false,
            alerted_decline: false,
            alerted_near_stall: false,
            last_alert_time: 0,
        }
    }

    fn add_sample(&mut self, speed_bps: f64) {
        self.speed_samples.push(speed_bps);
        // Keep only last 20 samples
        if self.speed_samples.len() > 20 {
            self.speed_samples.remove(0);
        }
    }
}

/// Speed alert manager
#[derive(Debug)]
pub struct SpeedAlertManager {
    config: Arc<RwLock<SpeedAlertConfig>>,
    monitors: Arc<RwLock<HashMap<String, TaskMonitorState>>>,
    alert_history: Arc<RwLock<Vec<SpeedAlert>>>,
    next_alert_id: Arc<RwLock<u64>>,
}

impl Default for SpeedAlertManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedAlertManager {
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SpeedAlertConfig::default())),
            monitors: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(RwLock::new(Vec::new())),
            next_alert_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Set configuration
    pub async fn set_config(&self, config: SpeedAlertConfig) {
        let mut cfg = self.config.write().await;
        *cfg = config;
    }

    /// Get current configuration
    pub async fn get_config(&self) -> SpeedAlertConfig {
        self.config.read().await.clone()
    }

    /// Record a speed sample for a task and check for alerts
    /// Returns any new alerts generated
    pub async fn record_speed(
        &self,
        task_id: &str,
        task_name: &str,
        speed_bps: f64,
        now_secs: u64,
    ) -> Vec<SpeedAlert> {
        let config = self.config.read().await;
        if !config.enabled {
            return vec![];
        }

        let mut monitors = self.monitors.write().await;
        let state = monitors
            .entry(task_id.to_string())
            .or_insert_with(TaskMonitorState::new);

        state.add_sample(speed_bps);

        let mut alerts = vec![];

        // Check cooldown
        let cooldown_ok =
            now_secs.saturating_sub(state.last_alert_time) >= config.alert_cooldown_secs;

        if !cooldown_ok {
            return vec![];
        }

        // Check near-stall
        if speed_bps < config.near_stall_bps && speed_bps >= 0.0 && !state.alerted_near_stall {
            let alert = self
                .create_alert(
                    task_id,
                    task_name,
                    AlertType::NearStall,
                    AlertSeverity::Critical,
                    format!(
                        "Speed dropped to near-stall level ({}/s < {}/s)",
                        format_speed(speed_bps),
                        format_speed(config.near_stall_bps)
                    ),
                    speed_bps,
                    Some(config.near_stall_bps),
                    now_secs,
                )
                .await;
            state.alerted_near_stall = true;
            state.last_alert_time = now_secs;
            alerts.push(alert);
        }

        // Check below threshold
        if let Some(min_speed) = config.min_speed_bps {
            if speed_bps < min_speed && speed_bps >= config.near_stall_bps {
                state.below_threshold_count += 1;
                if state.below_threshold_count >= config.min_speed_consecutive
                    && !state.alerted_below_threshold
                {
                    let alert = self
                        .create_alert(
                            task_id,
                            task_name,
                            AlertType::BelowThreshold,
                            AlertSeverity::Warning,
                            format!(
                                "Speed below threshold for {} consecutive samples ({}/s < {}/s)",
                                config.min_speed_consecutive,
                                format_speed(speed_bps),
                                format_speed(min_speed)
                            ),
                            speed_bps,
                            Some(min_speed),
                            now_secs,
                        )
                        .await;
                    state.alerted_below_threshold = true;
                    state.last_alert_time = now_secs;
                    alerts.push(alert);
                }
            } else if speed_bps >= min_speed {
                // Reset counter when speed recovers
                state.below_threshold_count = 0;
                state.alerted_below_threshold = false;
            }
        }

        // Check sustained decline
        if config.decline_detection && !state.alerted_decline {
            let samples = &state.speed_samples;
            if samples.len() >= config.decline_samples as usize {
                let recent = samples[samples.len() - config.decline_samples as usize..].to_vec();
                if self.is_sustained_decline(&recent, config.decline_ratio) {
                    let drop_pct = if recent[0] > 0.0 {
                        ((recent[0] - recent[recent.len() - 1]) / recent[0] * 100.0) as u32
                    } else {
                        0
                    };
                    let alert = self
                        .create_alert(
                            task_id,
                            task_name,
                            AlertType::SustainedDecline,
                            AlertSeverity::Info,
                            format!(
                                "Speed declined {}% over {} samples ({}/s → {}/s)",
                                drop_pct,
                                config.decline_samples,
                                format_speed(recent[0]),
                                format_speed(recent[recent.len() - 1])
                            ),
                            speed_bps,
                            None,
                            now_secs,
                        )
                        .await;
                    state.alerted_decline = true;
                    state.last_alert_time = now_secs;
                    alerts.push(alert);
                }
            }
        }

        // Reset decline alert when speed recovers
        if state.speed_samples.len() >= 2 {
            let last = state.speed_samples[state.speed_samples.len() - 1];
            let prev = state.speed_samples[state.speed_samples.len() - 2];
            if last > prev * 1.2 {
                state.alerted_decline = false;
            }
        }

        // Reset near-stall alert when speed recovers
        if speed_bps > config.near_stall_bps * 2.0 {
            state.alerted_near_stall = false;
        }

        alerts
    }

    /// Check if recent samples show a sustained decline
    fn is_sustained_decline(&self, samples: &[f64], decline_ratio: f64) -> bool {
        if samples.len() < 2 || samples[0] <= 0.0 {
            return false;
        }

        let first = samples[0];
        let last = samples[samples.len() - 1];

        // Overall decline ratio check
        if last >= first * (1.0 - decline_ratio) {
            return false;
        }

        // Check that decline is mostly monotonic (at least 60% of consecutive pairs decline)
        let declining_pairs = samples.windows(2).filter(|w| w[1] < w[0]).count();
        let total_pairs = samples.len() - 1;

        declining_pairs as f64 >= total_pairs as f64 * 0.6
    }

    /// Create an alert and add to history
    async fn create_alert(
        &self,
        task_id: &str,
        task_name: &str,
        alert_type: AlertType,
        severity: AlertSeverity,
        message: String,
        current_speed_bps: f64,
        threshold_bps: Option<f64>,
        timestamp: u64,
    ) -> SpeedAlert {
        let mut id = self.next_alert_id.write().await;
        let alert_id = *id;
        *id += 1;

        let alert = SpeedAlert {
            id: alert_id,
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            alert_type,
            severity,
            message,
            current_speed_bps,
            threshold_bps,
            timestamp,
        };

        // Add to history
        let mut history = self.alert_history.write().await;
        history.push(alert.clone());
        if history.len() > MAX_ALERT_HISTORY {
            history.remove(0);
        }

        alert
    }

    /// Get alert history
    pub async fn get_alerts(&self, limit: usize) -> Vec<SpeedAlert> {
        let history = self.alert_history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get alerts for a specific task
    pub async fn get_task_alerts(&self, task_id: &str, limit: usize) -> Vec<SpeedAlert> {
        let history = self.alert_history.read().await;
        history
            .iter()
            .rev()
            .filter(|a| a.task_id == task_id)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get alert summary
    pub async fn get_summary(&self) -> SpeedAlertSummary {
        let history = self.alert_history.read().await;
        let config = self.config.read().await;

        let total = history.len();
        let by_type = {
            let mut m: HashMap<String, usize> = HashMap::new();
            for a in &*history {
                *m.entry(a.alert_type.label().to_string()).or_default() += 1;
            }
            m
        };
        let by_severity = {
            let mut m: HashMap<String, usize> = HashMap::new();
            for a in &*history {
                *m.entry(a.severity.label().to_string()).or_default() += 1;
            }
            m
        };
        let affected_tasks = {
            let mut s = std::collections::HashSet::new();
            for a in &*history {
                s.insert(a.task_id.clone());
            }
            s.len()
        };

        SpeedAlertSummary {
            enabled: config.enabled,
            total_alerts: total,
            alerts_by_type: by_type,
            alerts_by_severity: by_severity,
            affected_tasks,
            recent_alerts: history.iter().rev().take(5).cloned().collect(),
        }
    }

    /// Clear alert history
    pub async fn clear_history(&self) {
        let mut history = self.alert_history.write().await;
        history.clear();
    }

    /// Remove monitoring state for a completed/removed task
    pub async fn remove_task(&self, task_id: &str) {
        let mut monitors = self.monitors.write().await;
        monitors.remove(task_id);
    }

    /// Clear all monitoring state
    pub async fn clear_monitors(&self) {
        let mut monitors = self.monitors.write().await;
        monitors.clear();
    }

    /// Save configuration to file
    pub async fn save_config(&self, path: &std::path::Path) -> Result<(), SpeedAlertError> {
        let config = self.config.read().await;
        let json = serde_json::to_string_pretty(&*config)
            .map_err(|e| SpeedAlertError::Serialize(e.to_string()))?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, json).map_err(|e| SpeedAlertError::Io(e.to_string()))?;
        std::fs::rename(&tmp, path).map_err(|e| SpeedAlertError::Io(e.to_string()))?;
        Ok(())
    }

    /// Load configuration from file
    pub async fn load_config(&self, path: &std::path::Path) -> Result<(), SpeedAlertError> {
        if !path.exists() {
            return Ok(());
        }
        let json = std::fs::read_to_string(path).map_err(|e| SpeedAlertError::Io(e.to_string()))?;
        let config: SpeedAlertConfig =
            serde_json::from_str(&json).map_err(|e| SpeedAlertError::Deserialize(e.to_string()))?;
        self.set_config(config).await;
        Ok(())
    }
}

/// Summary of speed alert status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedAlertSummary {
    pub enabled: bool,
    pub total_alerts: usize,
    pub alerts_by_type: HashMap<String, usize>,
    pub alerts_by_severity: HashMap<String, usize>,
    pub affected_tasks: usize,
    pub recent_alerts: Vec<SpeedAlert>,
}

impl SpeedAlertSummary {
    pub fn format_display(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Speed Alerts: {}\n",
            if self.enabled { "enabled" } else { "disabled" }
        ));
        out.push_str(&format!("Total alerts: {}\n", self.total_alerts));
        out.push_str(&format!("Affected tasks: {}\n", self.affected_tasks));
        if !self.alerts_by_type.is_empty() {
            out.push_str("By type:\n");
            for (k, v) in &self.alerts_by_type {
                out.push_str(&format!("  {}: {}\n", k, v));
            }
        }
        if !self.recent_alerts.is_empty() {
            out.push_str("\nRecent alerts:\n");
            for a in &self.recent_alerts {
                out.push_str(&format!("  {}\n", a.format_display()));
            }
        }
        out
    }
}

/// Errors for speed alert operations
#[derive(Debug, Clone)]
pub enum SpeedAlertError {
    Io(String),
    Serialize(String),
    Deserialize(String),
}

impl std::fmt::Display for SpeedAlertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Serialize(e) => write!(f, "Serialize error: {}", e),
            Self::Deserialize(e) => write!(f, "Deserialize error: {}", e),
        }
    }
}

impl std::error::Error for SpeedAlertError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SpeedAlertConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_speed_bps, Some(10_000.0));
        assert_eq!(config.min_speed_consecutive, 3);
        assert!(config.decline_detection);
        assert_eq!(config.near_stall_bps, 100.0);
    }

    #[test]
    fn test_alert_type_labels() {
        assert_eq!(AlertType::BelowThreshold.label(), "below_threshold");
        assert_eq!(AlertType::SustainedDecline.label(), "sustained_decline");
        assert_eq!(AlertType::NearStall.label(), "near_stall");
    }

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Critical > AlertSeverity::Warning);
        assert!(AlertSeverity::Warning > AlertSeverity::Info);
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500.0), "500 B");
        assert_eq!(format_speed(1500.0), "1.5 KB");
        assert_eq!(format_speed(2_500_000.0), "2.5 MB");
    }

    #[test]
    fn test_task_monitor_state_add_sample() {
        let mut state = TaskMonitorState::new();
        for i in 0..25 {
            state.add_sample(i as f64 * 100.0);
        }
        // Should keep only last 20
        assert_eq!(state.speed_samples.len(), 20);
        assert_eq!(state.speed_samples[0], 500.0); // 25-20=5th sample
    }

    #[tokio::test]
    async fn test_disabled_produces_no_alerts() {
        let mgr = SpeedAlertManager::new();
        let alerts = mgr.record_speed("t1", "test", 100.0, 1000).await;
        assert!(alerts.is_empty());
    }

    #[tokio::test]
    async fn test_below_threshold_alert() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 3;
        config.decline_detection = false;
        mgr.set_config(config).await;

        // First two samples below threshold - no alert yet
        let alerts = mgr.record_speed("t1", "test", 500.0, 1000).await;
        assert!(alerts.is_empty());
        let alerts = mgr.record_speed("t1", "test", 500.0, 1001).await;
        assert!(alerts.is_empty());

        // Third consecutive sample - alert!
        let alerts = mgr.record_speed("t1", "test", 500.0, 1002).await;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].alert_type, AlertType::BelowThreshold);
        assert_eq!(alerts[0].severity, AlertSeverity::Warning);
    }

    #[tokio::test]
    async fn test_near_stall_alert() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.near_stall_bps = 100.0;
        config.min_speed_bps = None;
        config.decline_detection = false;
        mgr.set_config(config).await;

        let alerts = mgr.record_speed("t1", "test", 50.0, 1000).await;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].alert_type, AlertType::NearStall);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
    }

    #[tokio::test]
    async fn test_sustained_decline_alert() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.decline_detection = true;
        config.decline_samples = 5;
        config.decline_ratio = 0.5;
        config.min_speed_bps = None;
        config.near_stall_bps = 0.0; // disable near-stall
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        // Feed declining speeds
        let speeds = [10000.0, 8000.0, 6000.0, 4000.0, 2000.0];
        let mut all_alerts = vec![];
        for (i, &speed) in speeds.iter().enumerate() {
            let alerts = mgr.record_speed("t1", "test", speed, 1000 + i as u64).await;
            all_alerts.extend(alerts);
        }

        assert!(!all_alerts.is_empty());
        let decline_alert = all_alerts
            .iter()
            .find(|a| a.alert_type == AlertType::SustainedDecline);
        assert!(decline_alert.is_some());
    }

    #[tokio::test]
    async fn test_cooldown_prevents_rapid_alerts() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 60;
        mgr.set_config(config).await;

        // First alert
        let alerts = mgr.record_speed("t1", "test", 500.0, 1000).await;
        assert_eq!(alerts.len(), 1);

        // Within cooldown - no alert
        let alerts = mgr.record_speed("t1", "test", 500.0, 1010).await;
        assert!(alerts.is_empty());

        // After cooldown - alert again (need to reset below_threshold first)
        // Speed recovers then drops again
        let alerts = mgr.record_speed("t1", "test", 2000.0, 1070).await;
        assert!(alerts.is_empty()); // recovery resets counter
        let alerts = mgr.record_speed("t1", "test", 500.0, 1071).await;
        assert_eq!(alerts.len(), 1);
    }

    #[tokio::test]
    async fn test_speed_recovery_resets_alert() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 2;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        // Two below threshold - alert
        mgr.record_speed("t1", "test", 500.0, 1000).await;
        let alerts = mgr.record_speed("t1", "test", 500.0, 1001).await;
        assert_eq!(alerts.len(), 1);

        // Speed recovers
        mgr.record_speed("t1", "test", 5000.0, 1002).await;

        // Speed drops again - should alert again (reset)
        mgr.record_speed("t1", "test", 500.0, 1003).await;
        let alerts = mgr.record_speed("t1", "test", 500.0, 1004).await;
        assert_eq!(alerts.len(), 1);
    }

    #[tokio::test]
    async fn test_alert_history() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        mgr.record_speed("t1", "task1", 500.0, 1000).await;
        mgr.record_speed("t2", "task2", 500.0, 1001).await;

        let history = mgr.get_alerts(10).await;
        assert_eq!(history.len(), 2);
        // Most recent first
        assert_eq!(history[0].task_id, "t2");
        assert_eq!(history[1].task_id, "t1");
    }

    #[tokio::test]
    async fn test_task_alerts_filter() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        mgr.record_speed("t1", "task1", 500.0, 1000).await;
        mgr.record_speed("t2", "task2", 500.0, 1001).await;
        mgr.record_speed("t1", "task1", 500.0, 1002).await;

        let t1_alerts = mgr.get_task_alerts("t1", 10).await;
        assert_eq!(t1_alerts.len(), 1); // alerted_below_threshold prevents duplicate until recovery

        let t2_alerts = mgr.get_task_alerts("t2", 10).await;
        assert_eq!(t2_alerts.len(), 1);
    }

    #[tokio::test]
    async fn test_summary() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        mgr.record_speed("t1", "task1", 500.0, 1000).await;
        mgr.record_speed("t2", "task2", 50.0, 1001).await; // near-stall

        let summary = mgr.get_summary().await;
        assert!(summary.enabled);
        assert_eq!(summary.total_alerts, 2);
        assert_eq!(summary.affected_tasks, 2);
        assert!(!summary.alerts_by_type.is_empty());
    }

    #[tokio::test]
    async fn test_clear_history() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        mgr.record_speed("t1", "task1", 500.0, 1000).await;
        assert_eq!(mgr.get_alerts(10).await.len(), 1);

        mgr.clear_history().await;
        assert_eq!(mgr.get_alerts(10).await.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_task() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        mgr.record_speed("t1", "task1", 500.0, 1000).await;
        mgr.remove_task("t1").await;

        // Task state cleared, so next sample starts fresh and generates a new alert
        let alerts = mgr.record_speed("t1", "task1", 500.0, 1001).await;
        assert_eq!(alerts.len(), 1);
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(50_000.0);
        config.near_stall_bps = 200.0;
        mgr.set_config(config).await;

        let tmp = std::env::temp_dir().join("speed_alert_test_config.json");
        mgr.save_config(&tmp).await.unwrap();

        let mgr2 = SpeedAlertManager::new();
        mgr2.load_config(&tmp).await.unwrap();
        let loaded = mgr2.get_config().await;
        assert!(loaded.enabled);
        assert_eq!(loaded.min_speed_bps, Some(50_000.0));
        assert_eq!(loaded.near_stall_bps, 200.0);

        std::fs::remove_file(&tmp).ok();
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let mgr = SpeedAlertManager::new();
        let result = mgr
            .load_config(std::path::Path::new("/nonexistent/path.json"))
            .await;
        assert!(result.is_ok()); // Should silently succeed
    }

    #[test]
    fn test_is_sustained_decline() {
        let mgr = SpeedAlertManager::new();
        // Clear decline: 10000 -> 2000 (80% drop)
        assert!(mgr.is_sustained_decline(&[10000.0, 8000.0, 5000.0, 3000.0, 2000.0], 0.5));
        // No decline: stable
        assert!(!mgr.is_sustained_decline(&[5000.0, 5000.0, 5000.0, 5000.0, 5000.0], 0.5));
        // No decline: increasing
        assert!(!mgr.is_sustained_decline(&[1000.0, 2000.0, 3000.0, 4000.0, 5000.0], 0.5));
        // Only 1 sample - too few
        assert!(!mgr.is_sustained_decline(&[10000.0], 0.5));
        // 2 samples with 80% drop - still detected as decline
        assert!(mgr.is_sustained_decline(&[10000.0, 2000.0], 0.5));
    }

    #[test]
    fn test_speed_alert_format_display() {
        let alert = SpeedAlert {
            id: 1,
            task_id: "t1".to_string(),
            task_name: "test_file.zip".to_string(),
            alert_type: AlertType::BelowThreshold,
            severity: AlertSeverity::Warning,
            message: "Speed below 10 KB/s".to_string(),
            current_speed_bps: 5000.0,
            threshold_bps: Some(10000.0),
            timestamp: 1000,
        };
        let display = alert.format_display();
        assert!(display.contains("test_file.zip"));
        assert!(display.contains("5.0 KB"));
    }

    #[test]
    fn test_summary_format_display() {
        let summary = SpeedAlertSummary {
            enabled: true,
            total_alerts: 5,
            alerts_by_type: HashMap::from([("below_threshold".to_string(), 3)]),
            alerts_by_severity: HashMap::from([("warning".to_string(), 3)]),
            affected_tasks: 2,
            recent_alerts: vec![],
        };
        let display = summary.format_display();
        assert!(display.contains("enabled"));
        assert!(display.contains("Total alerts: 5"));
    }

    #[test]
    fn test_config_serialization() {
        let config = SpeedAlertConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: SpeedAlertConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.enabled, config.enabled);
        assert_eq!(parsed.min_speed_bps, config.min_speed_bps);
    }

    #[tokio::test]
    async fn test_alert_id_incrementing() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        mgr.record_speed("t1", "task1", 500.0, 1000).await;
        mgr.record_speed("t2", "task2", 500.0, 1001).await;

        let history = mgr.get_alerts(10).await;
        assert_eq!(history[0].id, 2); // most recent
        assert_eq!(history[1].id, 1);
    }

    #[tokio::test]
    async fn test_max_alert_history() {
        let mgr = SpeedAlertManager::new();
        let mut config = SpeedAlertConfig::default();
        config.enabled = true;
        config.min_speed_bps = Some(1000.0);
        config.min_speed_consecutive = 1;
        config.decline_detection = false;
        config.alert_cooldown_secs = 0;
        mgr.set_config(config).await;

        // Generate many alerts
        for i in 0..250 {
            let task_id = format!("t{}", i);
            mgr.record_speed(&task_id, &format!("task{}", i), 500.0, 1000 + i as u64)
                .await;
        }

        let history = mgr.get_alerts(300).await;
        assert!(history.len() <= MAX_ALERT_HISTORY);
    }
}
