//! Download completion notification system
//!
//! Sends notifications when downloads complete or fail via multiple channels:
//! - Desktop notifications (via notify-send on Linux)
//! - Shell commands (user-defined scripts)
//! - Log file entries
//! - Webhook POST requests
//!
//! Also supports event subscription and filtering for programmatic access.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, RwLock};
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Notification trigger events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum NotificationEvent {
    /// Download completed successfully
    DownloadComplete,
    /// Download failed
    DownloadFailed,
    /// All downloads in queue finished
    QueueEmpty,
    /// Download reached a progress milestone threshold
    ProgressMilestone,
}

impl NotificationEvent {
    /// Human-readable label for templates
    pub fn label(&self) -> &'static str {
        match self {
            Self::DownloadComplete => "download_complete",
            Self::DownloadFailed => "download_failed",
            Self::QueueEmpty => "queue_empty",
            Self::ProgressMilestone => "progress_milestone",
        }
    }
}

/// Notification channel configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationChannel {
    /// Desktop notification (notify-send on Linux, osascript on macOS)
    Desktop,
    /// Execute a shell command with template variables
    Shell {
        /// Command template, e.g., "echo '{name} finished' >> ~/downloads.log"
        command: String,
    },
    /// Append to a log file
    LogFile {
        /// Path to log file
        path: PathBuf,
    },
    /// HTTP POST webhook
    Webhook {
        /// Webhook URL
        url: String,
        /// Optional secret for HMAC signature (not yet implemented)
        #[allow(dead_code)]
        secret: Option<String>,
    },
}

/// Notification configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotificationConfig {
    /// Enable/disable all notifications
    pub enabled: bool,
    /// List of notification channels to use
    pub channels: Vec<NotificationChannel>,
    /// Which events trigger notifications
    pub events: Vec<NotificationEvent>,
}

impl NotificationConfig {
    /// Create a disabled config
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            channels: Vec::new(),
            events: Vec::new(),
        }
    }

    /// Create a config with desktop notifications for completion
    pub fn desktop_complete() -> Self {
        Self {
            enabled: true,
            channels: vec![NotificationChannel::Desktop],
            events: vec![NotificationEvent::DownloadComplete],
        }
    }

    /// Check if an event should trigger a notification
    pub fn should_notify(&self, event: NotificationEvent) -> bool {
        self.enabled && self.events.contains(&event)
    }
}

/// Context passed to notification templates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationContext {
    /// Task ID
    pub task_id: String,
    /// File name
    pub name: String,
    /// File size in bytes
    pub size: u64,
    /// Downloaded bytes
    pub downloaded: u64,
    /// Protocol used
    pub protocol: String,
    /// Save path
    pub save_path: String,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Event type
    pub event: NotificationEvent,
}

impl NotificationContext {
    /// Render a template string with context variables
    pub fn render_template(&self, template: &str) -> String {
        template
            .replace("{task_id}", &self.task_id)
            .replace("{name}", &self.name)
            .replace("{size}", &self.size.to_string())
            .replace("{downloaded}", &self.downloaded.to_string())
            .replace("{protocol}", &self.protocol)
            .replace("{save_path}", &self.save_path)
            .replace("{error}", self.error.as_deref().unwrap_or(""))
            .replace("{event}", self.event.label())
    }
}

/// Notification filter for subscriptions
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationFilter {
    /// Filter by event types (empty = all events)
    pub events: Vec<NotificationEvent>,
    /// Filter by task IDs (empty = all tasks)
    pub task_ids: Vec<String>,
    /// Filter by tags (empty = all tags)
    pub tags: Vec<String>,
}

impl NotificationFilter {
    /// Create a filter that matches all notifications
    pub fn all() -> Self {
        Self::default()
    }

    /// Create a filter for specific event types
    pub fn events(events: Vec<NotificationEvent>) -> Self {
        Self {
            events,
            ..Default::default()
        }
    }

    /// Create a filter for specific task IDs
    pub fn task_ids(task_ids: Vec<String>) -> Self {
        Self {
            task_ids,
            ..Default::default()
        }
    }

    /// Check if a notification context matches this filter
    pub fn matches(&self, ctx: &NotificationContext, context_tags: &[String]) -> bool {
        // Check event filter
        if !self.events.is_empty() && !self.events.contains(&ctx.event) {
            return false;
        }

        // Check task ID filter
        if !self.task_ids.is_empty() && !self.task_ids.contains(&ctx.task_id) {
            return false;
        }

        // Check tag filter (any tag matches)
        if !self.tags.is_empty() && !self.tags.iter().any(|t| context_tags.contains(t)) {
            return false;
        }

        true
    }
}

/// Subscription to notification events
#[derive(Debug, Clone)]
pub struct NotificationSubscription {
    /// Unique subscription ID
    pub id: String,
    /// Filter for which events to receive
    pub filter: NotificationFilter,
    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl NotificationSubscription {
    /// Create a new subscription with the given filter
    pub fn new(filter: NotificationFilter) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            filter,
            created_at: chrono::Utc::now(),
        }
    }
}

/// Notification history entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationHistoryEntry {
    /// When the notification was sent
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Event type that triggered the notification
    pub event: NotificationEvent,
    /// Task ID
    pub task_id: String,
    /// Task name
    pub name: String,
    /// File size
    pub size: u64,
    /// Bytes downloaded
    pub downloaded: u64,
    /// Protocol used
    pub protocol: String,
    /// Save path
    pub save_path: String,
    /// Error message (if any)
    pub error: Option<String>,
    /// Tags associated with the task
    pub tags: Vec<String>,
}

/// Notification history manager
///
/// Stores recent notification events in memory for querying.
/// Maximum 100 entries, oldest are evicted when limit is reached.
#[derive(Debug, Clone)]
pub struct NotificationHistory {
    entries: Arc<RwLock<VecDeque<NotificationHistoryEntry>>>,
    max_entries: usize,
}

impl Default for NotificationHistory {
    fn default() -> Self {
        Self::new(100)
    }
}

impl NotificationHistory {
    /// Create a new history manager with the given capacity
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(VecDeque::new())),
            max_entries,
        }
    }

    /// Record a notification event
    pub fn record(&self, entry: NotificationHistoryEntry) {
        let mut entries = self.entries.write().unwrap();
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Get all history entries (newest first)
    pub fn get_all(&self) -> Vec<NotificationHistoryEntry> {
        let entries = self.entries.read().unwrap();
        entries.iter().rev().cloned().collect()
    }

    /// Get the number of entries in history
    pub fn len(&self) -> usize {
        let entries = self.entries.read().unwrap();
        entries.len()
    }

    /// Check if history is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all history entries
    pub fn clear(&self) {
        let mut entries = self.entries.write().unwrap();
        entries.clear();
    }
}

/// Notification dispatcher
pub struct NotificationDispatcher {
    config: RwLock<NotificationConfig>,
    subscriptions: RwLock<HashMap<String, NotificationSubscription>>,
    event_sender: broadcast::Sender<NotificationContext>,
    history: NotificationHistory,
}

impl NotificationDispatcher {
    /// Create a new dispatcher with the given config
    pub fn new(config: NotificationConfig) -> Self {
        let (event_sender, _) = broadcast::channel(100);
        Self {
            config: RwLock::new(config),
            subscriptions: RwLock::new(HashMap::new()),
            event_sender,
            history: NotificationHistory::default(),
        }
    }

    /// Get the notification history manager
    pub fn history(&self) -> &NotificationHistory {
        &self.history
    }

    /// Update the notification configuration
    pub fn update_config(&self, config: NotificationConfig) {
        let mut c = self.config.write().unwrap();
        *c = config;
    }

    /// Subscribe to notification events with a filter
    pub fn subscribe(&self, filter: NotificationFilter) -> NotificationSubscription {
        let subscription = NotificationSubscription::new(filter);
        let mut subs = self.subscriptions.write().unwrap();
        subs.insert(subscription.id.clone(), subscription.clone());
        subscription
    }

    /// Unsubscribe from notification events
    pub fn unsubscribe(&self, subscription_id: &str) -> bool {
        let mut subs = self.subscriptions.write().unwrap();
        subs.remove(subscription_id).is_some()
    }

    /// Get the number of active subscriptions
    pub fn subscription_count(&self) -> usize {
        let subs = self.subscriptions.read().unwrap();
        subs.len()
    }

    /// List all active subscriptions
    pub fn list_subscriptions(&self) -> Vec<NotificationSubscription> {
        let subs = self.subscriptions.read().unwrap();
        subs.values().cloned().collect()
    }

    /// Subscribe to the broadcast channel for receiving events
    pub fn subscribe_events(&self) -> broadcast::Receiver<NotificationContext> {
        self.event_sender.subscribe()
    }

    /// Send a notification for the given event and context
    pub async fn dispatch(&self, ctx: &NotificationContext) -> Result<(), NotificationError> {
        self.dispatch_with_tags(ctx, &[]).await
    }

    /// Send a notification for the given event and context with tags
    pub async fn dispatch_with_tags(
        &self,
        ctx: &NotificationContext,
        tags: &[String],
    ) -> Result<(), NotificationError> {
        let (channels, enabled, events) = {
            let config = self.config.read().unwrap();
            (
                config.channels.clone(),
                config.enabled,
                config.events.clone(),
            )
        };

        // Send to configured channels if enabled
        if enabled && events.contains(&ctx.event) {
            for channel in &channels {
                let result = match channel {
                    NotificationChannel::Desktop => self.send_desktop(ctx).await,
                    NotificationChannel::Shell { command } => self.send_shell(ctx, command).await,
                    NotificationChannel::LogFile { path } => self.send_log_file(ctx, path).await,
                    NotificationChannel::Webhook { url, .. } => self.send_webhook(ctx, url).await,
                };

                if let Err(e) = result {
                    tracing::warn!(error = %e, channel = ?channel, "Failed to send notification");
                }
            }
        }

        // Record to history
        self.history.record(NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: ctx.event,
            task_id: ctx.task_id.clone(),
            name: ctx.name.clone(),
            size: ctx.size,
            downloaded: ctx.downloaded,
            protocol: ctx.protocol.clone(),
            save_path: ctx.save_path.clone(),
            error: ctx.error.clone(),
            tags: tags.to_vec(),
        });

        // Send to subscribers (broadcast to all, they filter)
        // Ignore error if no receivers
        let _ = self.event_sender.send(ctx.clone());

        // Also dispatch to filtered subscriptions
        {
            let subs = self.subscriptions.read().unwrap();
            for subscription in subs.values() {
                if subscription.filter.matches(ctx, tags) {
                    // Subscription matched, event already sent via broadcast
                    tracing::debug!(
                        subscription_id = %subscription.id,
                        event = ?ctx.event,
                        "Notification matched subscription filter"
                    );
                }
            }
        }

        Ok(())
    }

    /// Send desktop notification
    async fn send_desktop(&self, ctx: &NotificationContext) -> Result<(), NotificationError> {
        let title = match ctx.event {
            NotificationEvent::DownloadComplete => "Download Complete",
            NotificationEvent::DownloadFailed => "Download Failed",
            NotificationEvent::QueueEmpty => "All Downloads Complete",
            NotificationEvent::ProgressMilestone => "Download Progress",
        };

        let body = format!(
            "{}\nSize: {}\nPath: {}",
            ctx.name,
            format_size(ctx.size),
            ctx.save_path
        );

        // Try notify-send (Linux)
        #[cfg(target_os = "linux")]
        {
            use tokio::process::Command;
            let status = Command::new("notify-send")
                .arg("-a")
                .arg("IPMsg-Torrent")
                .arg(title)
                .arg(&body)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;

            match status {
                Ok(s) if s.success() => return Ok(()),
                _ => {
                    // Fall through to try other methods
                }
            }
        }

        // Try osascript (macOS)
        #[cfg(target_os = "macos")]
        {
            use tokio::process::Command;
            let script = format!(
                "display notification \"{}\" with title \"{}\"",
                body.replace('"', "\\\""),
                title
            );
            let status = Command::new("osascript")
                .arg("-e")
                .arg(&script)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;

            if let Ok(s) = status {
                if s.success() {
                    return Ok(());
                }
            }
        }

        // Fallback: just log
        tracing::info!(title, body, "Desktop notification (fallback)");
        Ok(())
    }

    /// Send shell command notification
    async fn send_shell(
        &self,
        ctx: &NotificationContext,
        command_template: &str,
    ) -> Result<(), NotificationError> {
        use tokio::process::Command;

        let command = ctx.render_template(command_template);

        let status = Command::new("sh")
            .arg("-c")
            .arg(&command)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|e| NotificationError::Shell(e.to_string()))?;

        if !status.success() {
            return Err(NotificationError::Shell(format!(
                "Command exited with status: {}",
                status
            )));
        }

        Ok(())
    }

    /// Send log file notification
    async fn send_log_file(
        &self,
        ctx: &NotificationContext,
        path: &PathBuf,
    ) -> Result<(), NotificationError> {
        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
        let line = format!(
            "[{}] {} - {} ({})\n",
            timestamp,
            ctx.event.label(),
            ctx.name,
            format_size(ctx.size)
        );

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| NotificationError::Io(e.to_string()))?;

        file.write_all(line.as_bytes())
            .await
            .map_err(|e| NotificationError::Io(e.to_string()))?;

        Ok(())
    }

    /// Send webhook notification
    async fn send_webhook(
        &self,
        ctx: &NotificationContext,
        url: &str,
    ) -> Result<(), NotificationError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| NotificationError::Http(e.to_string()))?;

        let payload =
            serde_json::to_value(ctx).map_err(|e| NotificationError::Serialize(e.to_string()))?;

        let resp = client
            .post(url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| NotificationError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(NotificationError::Http(format!(
                "Webhook returned status: {}",
                resp.status()
            )));
        }

        Ok(())
    }
}

/// Notification errors
#[derive(Debug, thiserror::Error)]
pub enum NotificationError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("Shell command error: {0}")]
    Shell(String),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Serialization error: {0}")]
    Serialize(String),
}

/// Save notification configuration to disk
pub fn save_notification_config(
    config: &NotificationConfig,
    data_dir: &std::path::Path,
) -> Result<(), NotificationPersistenceError> {
    let config_path = data_dir.join("notification_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| NotificationPersistenceError::Serialize(e.to_string()))?;
    std::fs::write(&config_path, json)
        .map_err(|e| NotificationPersistenceError::Io(e.to_string()))?;
    Ok(())
}

/// Load notification configuration from disk
pub fn load_notification_config(
    data_dir: &std::path::Path,
) -> Result<Option<NotificationConfig>, NotificationPersistenceError> {
    let config_path = data_dir.join("notification_config.json");

    if !config_path.exists() {
        return Ok(None);
    }

    let json = std::fs::read_to_string(&config_path)
        .map_err(|e| NotificationPersistenceError::Io(e.to_string()))?;

    let config: NotificationConfig = serde_json::from_str(&json)
        .map_err(|e| NotificationPersistenceError::Deserialize(e.to_string()))?;

    Ok(Some(config))
}

/// Errors when persisting notification configuration
#[derive(Debug, thiserror::Error)]
pub enum NotificationPersistenceError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

/// Format file size for display
fn format_size(bytes: u64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_event_label() {
        assert_eq!(
            NotificationEvent::DownloadComplete.label(),
            "download_complete"
        );
        assert_eq!(NotificationEvent::DownloadFailed.label(), "download_failed");
        assert_eq!(NotificationEvent::QueueEmpty.label(), "queue_empty");
    }

    #[test]
    fn test_notification_config_disabled() {
        let config = NotificationConfig::disabled();
        assert!(!config.enabled);
        assert!(!config.should_notify(NotificationEvent::DownloadComplete));
    }

    #[test]
    fn test_notification_config_desktop_complete() {
        let config = NotificationConfig::desktop_complete();
        assert!(config.enabled);
        assert!(config.should_notify(NotificationEvent::DownloadComplete));
        assert!(!config.should_notify(NotificationEvent::DownloadFailed));
    }

    #[test]
    fn test_notification_context_render_template() {
        let ctx = NotificationContext {
            task_id: "abc123".into(),
            name: "test.iso".into(),
            size: 1_073_741_824,
            downloaded: 1_073_741_824,
            protocol: "Torrent".into(),
            save_path: "/downloads/test.iso".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };

        let template = "Downloaded {name} ({size} bytes) to {save_path}";
        let rendered = ctx.render_template(template);
        assert_eq!(
            rendered,
            "Downloaded test.iso (1073741824 bytes) to /downloads/test.iso"
        );
    }

    #[test]
    fn test_notification_context_render_with_error() {
        let ctx = NotificationContext {
            task_id: "xyz".into(),
            name: "failed.zip".into(),
            size: 1000,
            downloaded: 500,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: Some("Connection timeout".into()),
            event: NotificationEvent::DownloadFailed,
        };

        let template = "{name} failed: {error}";
        let rendered = ctx.render_template(template);
        assert_eq!(rendered, "failed.zip failed: Connection timeout");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1024), "1.00 KB");
        assert_eq!(format_size(1536), "1.50 KB");
        assert_eq!(format_size(1_048_576), "1.00 MB");
        assert_eq!(format_size(1_073_741_824), "1.00 GB");
    }

    #[tokio::test]
    async fn test_dispatcher_disabled() {
        let config = NotificationConfig::disabled();
        let dispatcher = NotificationDispatcher::new(config);

        let ctx = NotificationContext {
            task_id: "test".into(),
            name: "test.txt".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };

        // Should not error when disabled
        assert!(dispatcher.dispatch(&ctx).await.is_ok());
    }

    #[tokio::test]
    async fn test_dispatcher_log_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::LogFile {
                path: log_path.clone(),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };

        let dispatcher = NotificationDispatcher::new(config);

        let ctx = NotificationContext {
            task_id: "log-test".into(),
            name: "logged.txt".into(),
            size: 2048,
            downloaded: 2048,
            protocol: "Torrent".into(),
            save_path: "/downloads".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };

        dispatcher.dispatch(&ctx).await.unwrap();

        // Verify log file was created and contains the entry
        let content = tokio::fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("download_complete"));
        assert!(content.contains("logged.txt"));
        assert!(content.contains("2.00 KB"));
    }

    #[tokio::test]
    async fn test_dispatcher_shell_command() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.txt");

        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::Shell {
                command: format!("echo '{{name}} completed' > {}", output_path.display()),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };

        let dispatcher = NotificationDispatcher::new(config);

        let ctx = NotificationContext {
            task_id: "shell-test".into(),
            name: "shell_file.txt".into(),
            size: 1024,
            downloaded: 1024,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };

        dispatcher.dispatch(&ctx).await.unwrap();

        // Verify shell command was executed
        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert!(content.contains("shell_file.txt completed"));
    }

    #[test]
    fn test_notification_config_serialization() {
        let config = NotificationConfig {
            enabled: true,
            channels: vec![
                NotificationChannel::Desktop,
                NotificationChannel::LogFile {
                    path: PathBuf::from("/tmp/test.log"),
                },
            ],
            events: vec![
                NotificationEvent::DownloadComplete,
                NotificationEvent::DownloadFailed,
            ],
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NotificationConfig = serde_json::from_str(&json).unwrap();

        assert!(deserialized.enabled);
        assert_eq!(deserialized.channels.len(), 2);
        assert_eq!(deserialized.events.len(), 2);
    }

    // ===== Phase 37: Subscription and Filtering Tests =====

    #[test]
    fn test_notification_filter_all() {
        let filter = NotificationFilter::all();
        let ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &[]));
        assert!(filter.matches(&ctx, &["tag1".into(), "tag2".into()]));
    }

    #[test]
    fn test_notification_filter_by_event() {
        let filter = NotificationFilter::events(vec![NotificationEvent::DownloadComplete]);

        let complete_ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        let failed_ctx = make_test_ctx("task2", "file2.txt", NotificationEvent::DownloadFailed);

        assert!(filter.matches(&complete_ctx, &[]));
        assert!(!filter.matches(&failed_ctx, &[]));
    }

    #[test]
    fn test_notification_filter_by_task_id() {
        let filter = NotificationFilter::task_ids(vec!["task1".into(), "task3".into()]);

        let ctx1 = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        let ctx2 = make_test_ctx("task2", "file2.txt", NotificationEvent::DownloadComplete);
        let ctx3 = make_test_ctx("task3", "file3.txt", NotificationEvent::DownloadFailed);

        assert!(filter.matches(&ctx1, &[]));
        assert!(!filter.matches(&ctx2, &[]));
        assert!(filter.matches(&ctx3, &[]));
    }

    #[test]
    fn test_notification_filter_by_tags() {
        let filter = NotificationFilter {
            tags: vec!["important".into(), "urgent".into()],
            ..Default::default()
        };

        let ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);

        // No tags - should not match
        assert!(!filter.matches(&ctx, &[]));

        // Has matching tag
        assert!(filter.matches(&ctx, &["important".into()]));
        assert!(filter.matches(&ctx, &["urgent".into(), "other".into()]));

        // No matching tag
        assert!(!filter.matches(&ctx, &["other".into(), "misc".into()]));
    }

    #[test]
    fn test_notification_filter_combined() {
        let filter = NotificationFilter {
            events: vec![NotificationEvent::DownloadComplete],
            task_ids: vec!["task1".into()],
            tags: vec!["important".into()],
        };

        // All match
        let ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &["important".into()]));

        // Wrong event
        let ctx_wrong_event = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadFailed);
        assert!(!filter.matches(&ctx_wrong_event, &["important".into()]));

        // Wrong task ID
        let ctx_wrong_task =
            make_test_ctx("task2", "file.txt", NotificationEvent::DownloadComplete);
        assert!(!filter.matches(&ctx_wrong_task, &["important".into()]));

        // Wrong tag
        let ctx_wrong_tag = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        assert!(!filter.matches(&ctx_wrong_tag, &["other".into()]));
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());

        assert_eq!(dispatcher.subscription_count(), 0);

        let sub1 = dispatcher.subscribe(NotificationFilter::all());
        assert_eq!(dispatcher.subscription_count(), 1);

        let sub2 = dispatcher.subscribe(NotificationFilter::events(vec![
            NotificationEvent::DownloadComplete,
        ]));
        assert_eq!(dispatcher.subscription_count(), 2);

        // Verify subscriptions are listed
        let subs = dispatcher.list_subscriptions();
        assert_eq!(subs.len(), 2);

        // Unsubscribe
        assert!(dispatcher.unsubscribe(&sub1.id));
        assert_eq!(dispatcher.subscription_count(), 1);

        assert!(dispatcher.unsubscribe(&sub2.id));
        assert_eq!(dispatcher.subscription_count(), 0);

        // Unsubscribing non-existent returns false
        assert!(!dispatcher.unsubscribe("nonexistent"));
    }

    #[tokio::test]
    async fn test_dispatch_broadcast_to_subscribers() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());

        // Subscribe with a receiver
        let mut receiver = dispatcher.subscribe_events();

        let ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        dispatcher.dispatch(&ctx).await.unwrap();

        // Receiver should get the event
        let received = receiver.try_recv().unwrap();
        assert_eq!(received.task_id, "task1");
        assert_eq!(received.event, NotificationEvent::DownloadComplete);
    }

    #[test]
    fn test_notification_filter_serialization() {
        let filter = NotificationFilter {
            events: vec![
                NotificationEvent::DownloadComplete,
                NotificationEvent::DownloadFailed,
            ],
            task_ids: vec!["task1".into(), "task2".into()],
            tags: vec!["important".into()],
        };

        let json = serde_json::to_string(&filter).unwrap();
        let deserialized: NotificationFilter = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.events.len(), 2);
        assert_eq!(deserialized.task_ids.len(), 2);
        assert_eq!(deserialized.tags.len(), 1);

        // Verify it still works after deserialization
        let ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        assert!(deserialized.matches(&ctx, &["important".into()]));
    }

    /// Helper to create a test context
    fn make_test_ctx(task_id: &str, name: &str, event: NotificationEvent) -> NotificationContext {
        NotificationContext {
            task_id: task_id.into(),
            name: name.into(),
            size: 1024,
            downloaded: 1024,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            event,
        }
    }

    // ===== Phase 38: Notification Persistence Tests =====

    #[test]
    fn test_notification_history_record() {
        let history = NotificationHistory::new(100);
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);

        let entry = NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadComplete,
            task_id: "task1".into(),
            name: "file.txt".into(),
            size: 1024,
            downloaded: 1024,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            tags: vec!["tag1".into()],
        };

        history.record(entry);
        assert_eq!(history.len(), 1);
        assert!(!history.is_empty());
    }

    #[test]
    fn test_notification_history_eviction() {
        let history = NotificationHistory::new(3);

        for i in 0..5 {
            history.record(NotificationHistoryEntry {
                timestamp: chrono::Utc::now(),
                event: NotificationEvent::DownloadComplete,
                task_id: format!("task{}", i),
                name: format!("file{}.txt", i),
                size: 1024,
                downloaded: 1024,
                protocol: "HTTP".into(),
                save_path: "/tmp".into(),
                error: None,
                tags: vec![],
            });
        }

        // Should only keep the last 3 entries
        assert_eq!(history.len(), 3);
        let all = history.get_all();
        assert_eq!(all[0].task_id, "task4"); // newest first
        assert_eq!(all[1].task_id, "task3");
        assert_eq!(all[2].task_id, "task2");
    }

    #[test]
    fn test_notification_history_clear() {
        let history = NotificationHistory::new(100);

        for i in 0..5 {
            history.record(NotificationHistoryEntry {
                timestamp: chrono::Utc::now(),
                event: NotificationEvent::DownloadComplete,
                task_id: format!("task{}", i),
                name: "test".into(),
                size: 1024,
                downloaded: 1024,
                protocol: "HTTP".into(),
                save_path: "/tmp".into(),
                error: None,
                tags: vec![],
            });
        }

        assert_eq!(history.len(), 5);
        history.clear();
        assert_eq!(history.len(), 0);
        assert!(history.is_empty());
    }

    #[tokio::test]
    async fn test_dispatch_records_to_history() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());

        let ctx = make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
        dispatcher.dispatch(&ctx).await.unwrap();

        let history = dispatcher.history();
        assert_eq!(history.len(), 1);

        let entries = history.get_all();
        assert_eq!(entries[0].task_id, "task1");
        assert_eq!(entries[0].name, "file.txt");
        assert_eq!(entries[0].event, NotificationEvent::DownloadComplete);
    }

    #[test]
    fn test_save_load_notification_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // No config file initially
        let loaded = load_notification_config(data_dir).unwrap();
        assert!(loaded.is_none());

        // Save a config
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::Desktop],
            events: vec![
                NotificationEvent::DownloadComplete,
                NotificationEvent::DownloadFailed,
            ],
        };

        save_notification_config(&config, data_dir).unwrap();

        // Load it back
        let loaded = load_notification_config(data_dir).unwrap().unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.channels.len(), 1);
        assert_eq!(loaded.events.len(), 2);
    }

    #[test]
    fn test_notification_history_entry_serialization() {
        let entry = NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadFailed,
            task_id: "task-123".into(),
            name: "broken.zip".into(),
            size: 1024 * 1024,
            downloaded: 512,
            protocol: "Torrent".into(),
            save_path: "/downloads".into(),
            error: Some("Connection reset".into()),
            tags: vec!["important".into(), "large".into()],
        };

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: NotificationHistoryEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task_id, "task-123");
        assert_eq!(deserialized.event, NotificationEvent::DownloadFailed);
        assert_eq!(deserialized.error, Some("Connection reset".into()));
        assert_eq!(deserialized.tags.len(), 2);
    }

    // ===== Phase 241: Notification Comprehensive Test Coverage =====

    // --- NotificationEvent: all variants ---

    #[test]
    fn event_label_all_variants() {
        assert_eq!(
            NotificationEvent::DownloadComplete.label(),
            "download_complete"
        );
        assert_eq!(NotificationEvent::DownloadFailed.label(), "download_failed");
        assert_eq!(NotificationEvent::QueueEmpty.label(), "queue_empty");
        assert_eq!(
            NotificationEvent::ProgressMilestone.label(),
            "progress_milestone"
        );
    }

    #[test]
    fn event_serde_roundtrip_all_variants() {
        for event in [
            NotificationEvent::DownloadComplete,
            NotificationEvent::DownloadFailed,
            NotificationEvent::QueueEmpty,
            NotificationEvent::ProgressMilestone,
        ] {
            let json = serde_json::to_string(&event).unwrap();
            let loaded: NotificationEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(loaded, event);
        }
    }

    #[test]
    fn event_serde_pascal_case_values() {
        assert_eq!(
            serde_json::to_string(&NotificationEvent::DownloadComplete).unwrap(),
            "\"DownloadComplete\""
        );
        assert_eq!(
            serde_json::to_string(&NotificationEvent::QueueEmpty).unwrap(),
            "\"QueueEmpty\""
        );
    }

    #[test]
    fn event_clone_copy_debug_eq() {
        let e = NotificationEvent::DownloadComplete;
        let e2 = e;
        assert_eq!(e, e2);
        let s = format!("{:?}", e);
        assert!(s.contains("DownloadComplete"));
    }

    #[test]
    fn event_hash_trait() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(NotificationEvent::DownloadComplete);
        set.insert(NotificationEvent::DownloadFailed);
        set.insert(NotificationEvent::DownloadComplete);
        assert_eq!(set.len(), 2);
    }

    // --- NotificationChannel serde ---

    #[test]
    fn channel_serde_desktop() {
        let ch = NotificationChannel::Desktop;
        let json = serde_json::to_string(&ch).unwrap();
        let loaded: NotificationChannel = serde_json::from_str(&json).unwrap();
        assert!(matches!(loaded, NotificationChannel::Desktop));
    }

    #[test]
    fn channel_serde_shell() {
        let ch = NotificationChannel::Shell {
            command: "echo done".into(),
        };
        let json = serde_json::to_string(&ch).unwrap();
        let loaded: NotificationChannel = serde_json::from_str(&json).unwrap();
        match loaded {
            NotificationChannel::Shell { command } => assert_eq!(command, "echo done"),
            _ => panic!("expected Shell"),
        }
    }

    #[test]
    fn channel_serde_logfile() {
        let ch = NotificationChannel::LogFile {
            path: PathBuf::from("/tmp/test.log"),
        };
        let json = serde_json::to_string(&ch).unwrap();
        let loaded: NotificationChannel = serde_json::from_str(&json).unwrap();
        match loaded {
            NotificationChannel::LogFile { path } => {
                assert_eq!(path, PathBuf::from("/tmp/test.log"))
            }
            _ => panic!("expected LogFile"),
        }
    }

    #[test]
    fn channel_serde_webhook() {
        let ch = NotificationChannel::Webhook {
            url: "https://example.com/hook".into(),
            secret: Some("s3cret".into()),
        };
        let json = serde_json::to_string(&ch).unwrap();
        let loaded: NotificationChannel = serde_json::from_str(&json).unwrap();
        match loaded {
            NotificationChannel::Webhook { url, secret } => {
                assert_eq!(url, "https://example.com/hook");
                assert_eq!(secret, Some("s3cret".into()));
            }
            _ => panic!("expected Webhook"),
        }
    }

    #[test]
    fn channel_clone_debug() {
        let ch = NotificationChannel::Desktop;
        let ch2 = ch.clone();
        assert!(matches!(ch2, NotificationChannel::Desktop));
        let s = format!("{:?}", ch);
        assert!(s.contains("Desktop"));
    }

    // --- NotificationConfig ---

    #[test]
    fn config_default() {
        let config = NotificationConfig::default();
        assert!(!config.enabled);
        assert!(config.channels.is_empty());
        assert!(config.events.is_empty());
    }

    #[test]
    fn config_serde_roundtrip_custom() {
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::Desktop],
            events: vec![
                NotificationEvent::DownloadComplete,
                NotificationEvent::DownloadFailed,
            ],
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: NotificationConfig = serde_json::from_str(&json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.channels.len(), 1);
        assert_eq!(loaded.events.len(), 2);
    }

    #[test]
    fn config_serde_pretty() {
        let config = NotificationConfig::desktop_complete();
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let loaded: NotificationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[test]
    fn config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"channels":[],"events":[],"unknown_field":42}"#;
        let loaded: NotificationConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
    }

    #[test]
    fn config_clone_debug() {
        let config = NotificationConfig::desktop_complete();
        let config2 = config.clone();
        assert_eq!(config.enabled, config2.enabled);
        let s = format!("{:?}", config);
        assert!(s.contains("enabled"));
    }

    #[test]
    fn config_should_notify_all_events() {
        let config = NotificationConfig {
            enabled: true,
            channels: vec![],
            events: vec![
                NotificationEvent::DownloadComplete,
                NotificationEvent::DownloadFailed,
                NotificationEvent::QueueEmpty,
                NotificationEvent::ProgressMilestone,
            ],
        };
        assert!(config.should_notify(NotificationEvent::DownloadComplete));
        assert!(config.should_notify(NotificationEvent::DownloadFailed));
        assert!(config.should_notify(NotificationEvent::QueueEmpty));
        assert!(config.should_notify(NotificationEvent::ProgressMilestone));
    }

    #[test]
    fn config_disabled_never_notifies() {
        let config = NotificationConfig {
            enabled: false,
            channels: vec![NotificationChannel::Desktop],
            events: vec![NotificationEvent::DownloadComplete],
        };
        assert!(!config.should_notify(NotificationEvent::DownloadComplete));
    }

    // --- NotificationContext ---

    #[test]
    fn context_serde_roundtrip() {
        let ctx = NotificationContext {
            task_id: "t1".into(),
            name: "file.txt".into(),
            size: 1024,
            downloaded: 512,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: Some("timeout".into()),
            event: NotificationEvent::DownloadFailed,
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let loaded: NotificationContext = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, "t1");
        assert_eq!(loaded.error, Some("timeout".into()));
    }

    #[test]
    fn context_serde_none_error() {
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(json.contains("null") || !json.contains("error"));
        let loaded: NotificationContext = serde_json::from_str(&json).unwrap();
        assert!(loaded.error.is_none());
    }

    #[test]
    fn context_clone_debug() {
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        let ctx2 = ctx.clone();
        assert_eq!(ctx2.task_id, "t1");
        let s = format!("{:?}", ctx);
        assert!(s.contains("t1"));
    }

    #[test]
    fn context_render_template_unicode() {
        let ctx = NotificationContext {
            task_id: "任务-001".into(),
            name: "中文文件.txt".into(),
            size: 1024,
            downloaded: 1024,
            protocol: "HTTP".into(),
            save_path: "/下载/文件.txt".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };
        let rendered = ctx.render_template("{name} -> {save_path}");
        assert_eq!(rendered, "中文文件.txt -> /下载/文件.txt");
    }

    #[test]
    fn context_render_template_emoji() {
        let ctx = NotificationContext {
            task_id: "🚀".into(),
            name: "🎉.txt".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp/🎉".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };
        let rendered = ctx.render_template("{task_id}: {name}");
        assert_eq!(rendered, "🚀: 🎉.txt");
    }

    #[test]
    fn context_render_template_empty_error() {
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        let rendered = ctx.render_template("Error: {error}");
        assert_eq!(rendered, "Error: ");
    }

    #[test]
    fn context_render_template_all_placeholders() {
        let ctx = NotificationContext {
            task_id: "id1".into(),
            name: "n".into(),
            size: 10,
            downloaded: 5,
            protocol: "P".into(),
            save_path: "/s".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };
        let rendered =
            ctx.render_template("{task_id}{name}{size}{downloaded}{protocol}{save_path}{event}");
        assert_eq!(rendered, "id1n105P/sdownload_complete");
    }

    // --- NotificationFilter ---

    #[test]
    fn filter_default_is_all() {
        let filter = NotificationFilter::default();
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &[]));
        assert!(filter.matches(&ctx, &["any".into()]));
    }

    #[test]
    fn filter_events_only() {
        let filter = NotificationFilter::events(vec![NotificationEvent::DownloadFailed]);
        let complete = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        let failed = make_test_ctx("t2", "f.txt", NotificationEvent::DownloadFailed);
        assert!(!filter.matches(&complete, &[]));
        assert!(filter.matches(&failed, &[]));
    }

    #[test]
    fn filter_task_ids_only() {
        let filter = NotificationFilter::task_ids(vec!["t1".into()]);
        let ctx1 = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        let ctx2 = make_test_ctx("t2", "f.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx1, &[]));
        assert!(!filter.matches(&ctx2, &[]));
    }

    #[test]
    fn filter_tags_any_match() {
        let filter = NotificationFilter {
            tags: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &["a".into()]));
        assert!(filter.matches(&ctx, &["b".into(), "c".into()]));
        assert!(!filter.matches(&ctx, &["c".into()]));
        assert!(!filter.matches(&ctx, &[]));
    }

    #[test]
    fn filter_empty_tags_empty_context_tags() {
        let filter = NotificationFilter {
            tags: vec!["x".into()],
            ..Default::default()
        };
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        assert!(!filter.matches(&ctx, &[]));
    }

    #[test]
    fn filter_serde_roundtrip() {
        let filter = NotificationFilter {
            events: vec![NotificationEvent::DownloadComplete],
            task_ids: vec!["t1".into()],
            tags: vec!["tag1".into()],
        };
        let json = serde_json::to_string(&filter).unwrap();
        let loaded: NotificationFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.events.len(), 1);
        assert_eq!(loaded.task_ids.len(), 1);
        assert_eq!(loaded.tags.len(), 1);
    }

    #[test]
    fn filter_serde_extra_fields_ignored() {
        let json = r#"{"events":[],"task_ids":[],"tags":[],"extra":true}"#;
        let loaded: NotificationFilter = serde_json::from_str(json).unwrap();
        assert!(loaded.events.is_empty());
    }

    #[test]
    fn filter_clone_debug() {
        let filter = NotificationFilter::all();
        let filter2 = filter.clone();
        assert!(filter2.task_ids.is_empty());
        let s = format!("{:?}", filter);
        assert!(s.contains("events"));
    }

    #[test]
    fn filter_unicode_task_ids() {
        let filter = NotificationFilter::task_ids(vec!["任务-中文".into()]);
        let ctx = make_test_ctx("任务-中文", "f.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &[]));
    }

    #[test]
    fn filter_emoji_task_ids() {
        let filter = NotificationFilter::task_ids(vec!["🚀".into()]);
        let ctx = make_test_ctx("🚀", "f.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &[]));
    }

    // --- NotificationSubscription ---

    #[test]
    fn subscription_new_has_unique_id() {
        let sub1 = NotificationSubscription::new(NotificationFilter::all());
        let sub2 = NotificationSubscription::new(NotificationFilter::all());
        assert_ne!(sub1.id, sub2.id);
    }

    #[test]
    fn subscription_new_has_created_at() {
        let sub = NotificationSubscription::new(NotificationFilter::all());
        assert!(sub.created_at <= chrono::Utc::now());
    }

    #[test]
    fn subscription_clone_debug() {
        let sub = NotificationSubscription::new(NotificationFilter::all());
        let sub2 = sub.clone();
        assert_eq!(sub.id, sub2.id);
        let s = format!("{:?}", sub);
        assert!(s.contains("id"));
    }

    // --- NotificationHistory ---

    #[test]
    fn history_default_max_100() {
        let history = NotificationHistory::default();
        assert_eq!(history.len(), 0);
        for i in 0..150 {
            history.record(NotificationHistoryEntry {
                timestamp: chrono::Utc::now(),
                event: NotificationEvent::DownloadComplete,
                task_id: format!("t{}", i),
                name: "f.txt".into(),
                size: 100,
                downloaded: 100,
                protocol: "HTTP".into(),
                save_path: "/tmp".into(),
                error: None,
                tags: vec![],
            });
        }
        assert_eq!(history.len(), 100);
    }

    #[test]
    fn history_max_1() {
        let history = NotificationHistory::new(1);
        history.record(NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadComplete,
            task_id: "first".into(),
            name: "f.txt".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            tags: vec![],
        });
        history.record(NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadFailed,
            task_id: "second".into(),
            name: "f.txt".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            tags: vec![],
        });
        assert_eq!(history.len(), 1);
        let all = history.get_all();
        assert_eq!(all[0].task_id, "second");
    }

    #[test]
    fn history_get_all_newest_first() {
        let history = NotificationHistory::new(100);
        for i in 0..3 {
            history.record(NotificationHistoryEntry {
                timestamp: chrono::Utc::now(),
                event: NotificationEvent::DownloadComplete,
                task_id: format!("t{}", i),
                name: "f.txt".into(),
                size: 100,
                downloaded: 100,
                protocol: "HTTP".into(),
                save_path: "/tmp".into(),
                error: None,
                tags: vec![],
            });
        }
        let all = history.get_all();
        assert_eq!(all[0].task_id, "t2");
        assert_eq!(all[1].task_id, "t1");
        assert_eq!(all[2].task_id, "t0");
    }

    #[test]
    fn history_clone_debug() {
        let history = NotificationHistory::new(10);
        let history2 = history.clone();
        assert_eq!(history2.len(), 0);
        let s = format!("{:?}", history);
        assert!(s.contains("entries"));
    }

    // --- NotificationHistoryEntry serde ---

    #[test]
    fn history_entry_serde_roundtrip() {
        let entry = NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::QueueEmpty,
            task_id: "t1".into(),
            name: "f.txt".into(),
            size: 1024,
            downloaded: 1024,
            protocol: "Torrent".into(),
            save_path: "/dl".into(),
            error: None,
            tags: vec!["a".into(), "b".into()],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let loaded: NotificationHistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, "t1");
        assert_eq!(loaded.tags.len(), 2);
    }

    #[test]
    fn history_entry_clone_debug() {
        let entry = NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadComplete,
            task_id: "t1".into(),
            name: "f.txt".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            tags: vec![],
        };
        let entry2 = entry.clone();
        assert_eq!(entry2.task_id, "t1");
        let s = format!("{:?}", entry);
        assert!(s.contains("t1"));
    }

    #[test]
    fn history_entry_unicode_fields() {
        let entry = NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadComplete,
            task_id: "中文任务".into(),
            name: "日本語ファイル.txt".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp/🎉".into(),
            error: Some("错误消息".into()),
            tags: vec!["标签".into()],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let loaded: NotificationHistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, "中文任务");
        assert_eq!(loaded.name, "日本語ファイル.txt");
        assert_eq!(loaded.error, Some("错误消息".into()));
    }

    // --- NotificationError ---

    #[test]
    fn error_display_all_variants() {
        assert_eq!(
            NotificationError::Io("disk full".into()).to_string(),
            "IO error: disk full"
        );
        assert_eq!(
            NotificationError::Shell("exit 1".into()).to_string(),
            "Shell command error: exit 1"
        );
        assert_eq!(
            NotificationError::Http("404".into()).to_string(),
            "HTTP error: 404"
        );
        assert_eq!(
            NotificationError::Serialize("bad json".into()).to_string(),
            "Serialization error: bad json"
        );
    }

    #[test]
    fn error_debug_trait() {
        let e = NotificationError::Io("test".into());
        let s = format!("{:?}", e);
        assert!(s.contains("Io"));
    }

    #[test]
    fn error_unicode_messages() {
        let e = NotificationError::Io("磁盘已满".into());
        assert!(e.to_string().contains("磁盘已满"));
    }

    // --- NotificationPersistenceError ---

    #[test]
    fn persistence_error_display_all_variants() {
        assert_eq!(
            NotificationPersistenceError::Io("fail".into()).to_string(),
            "IO error: fail"
        );
        assert_eq!(
            NotificationPersistenceError::Serialize("bad".into()).to_string(),
            "serialize error: bad"
        );
        assert_eq!(
            NotificationPersistenceError::Deserialize("corrupt".into()).to_string(),
            "deserialize error: corrupt"
        );
    }

    #[test]
    fn persistence_error_debug_trait() {
        let e = NotificationPersistenceError::Io("test".into());
        let s = format!("{:?}", e);
        assert!(s.contains("Io"));
    }

    // --- format_size ---

    #[test]
    fn format_size_zero() {
        assert_eq!(format_size(0), "0 B");
    }

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(1), "1 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn format_size_exact_kb() {
        assert_eq!(format_size(1024), "1.00 KB");
    }

    #[test]
    fn format_size_exact_mb() {
        assert_eq!(format_size(1024 * 1024), "1.00 MB");
    }

    #[test]
    fn format_size_exact_gb() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn format_size_fractional_kb() {
        assert_eq!(format_size(1536), "1.50 KB");
    }

    #[test]
    fn format_size_large_value() {
        assert_eq!(format_size(5 * 1024 * 1024 * 1024), "5.00 GB");
    }

    // --- save/load notification config ---

    #[test]
    fn save_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = NotificationConfig::desktop_complete();
        save_notification_config(&config, temp_dir.path()).unwrap();
        assert!(temp_dir.path().join("notification_config.json").exists());
    }

    #[test]
    fn save_overwrites_existing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config1 = NotificationConfig::disabled();
        save_notification_config(&config1, temp_dir.path()).unwrap();
        let config2 = NotificationConfig::desktop_complete();
        save_notification_config(&config2, temp_dir.path()).unwrap();
        let loaded = load_notification_config(temp_dir.path()).unwrap().unwrap();
        assert!(loaded.enabled);
    }

    #[test]
    fn load_missing_file_returns_none() {
        let temp_dir = tempfile::tempdir().unwrap();
        let loaded = load_notification_config(temp_dir.path()).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn load_corrupt_json_returns_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("notification_config.json");
        std::fs::write(&path, "not json").unwrap();
        let result = load_notification_config(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn load_empty_file_returns_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("notification_config.json");
        std::fs::write(&path, "").unwrap();
        let result = load_notification_config(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn save_load_pretty_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = NotificationConfig {
            enabled: true,
            channels: vec![
                NotificationChannel::Desktop,
                NotificationChannel::LogFile {
                    path: PathBuf::from("/tmp/test.log"),
                },
            ],
            events: vec![NotificationEvent::DownloadComplete],
        };
        save_notification_config(&config, temp_dir.path()).unwrap();
        let loaded = load_notification_config(temp_dir.path()).unwrap().unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.channels.len(), 2);
        assert_eq!(loaded.events.len(), 1);
    }

    #[test]
    fn save_load_unicode_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::Shell {
                command: "echo '中文通知 🎉'".into(),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };
        save_notification_config(&config, temp_dir.path()).unwrap();
        let loaded = load_notification_config(temp_dir.path()).unwrap().unwrap();
        match &loaded.channels[0] {
            NotificationChannel::Shell { command } => {
                assert!(command.contains("中文"));
            }
            _ => panic!("expected Shell"),
        }
    }

    // --- NotificationDispatcher ---

    #[test]
    fn dispatcher_new_has_zero_subscriptions() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        assert_eq!(dispatcher.subscription_count(), 0);
    }

    #[test]
    fn dispatcher_update_config() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let new_config = NotificationConfig::desktop_complete();
        dispatcher.update_config(new_config);
        // Config is internal; verify via dispatch behavior
    }

    #[test]
    fn dispatcher_history_accessible() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        assert!(dispatcher.history().is_empty());
    }

    #[tokio::test]
    async fn dispatcher_dispatch_disabled_still_records_history() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        dispatcher.dispatch(&ctx).await.unwrap();
        assert_eq!(dispatcher.history().len(), 1);
    }

    #[tokio::test]
    async fn dispatcher_dispatch_multiple_accumulates_history() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        for i in 0..5 {
            let ctx = make_test_ctx(
                &format!("t{}", i),
                "f.txt",
                NotificationEvent::DownloadComplete,
            );
            dispatcher.dispatch(&ctx).await.unwrap();
        }
        assert_eq!(dispatcher.history().len(), 5);
    }

    #[tokio::test]
    async fn dispatcher_subscribe_events_receives_broadcast() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let mut rx = dispatcher.subscribe_events();
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadFailed);
        dispatcher.dispatch(&ctx).await.unwrap();
        let received = rx.try_recv().unwrap();
        assert_eq!(received.event, NotificationEvent::DownloadFailed);
    }

    #[tokio::test]
    async fn dispatcher_multiple_subscribers_all_receive() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let mut rx1 = dispatcher.subscribe_events();
        let mut rx2 = dispatcher.subscribe_events();
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::QueueEmpty);
        dispatcher.dispatch(&ctx).await.unwrap();
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[tokio::test]
    async fn dispatcher_dispatch_with_tags_records_tags_in_history() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        dispatcher
            .dispatch_with_tags(&ctx, &["tag1".into(), "tag2".into()])
            .await
            .unwrap();
        let entries = dispatcher.history().get_all();
        assert_eq!(entries[0].tags, vec!["tag1", "tag2"]);
    }

    #[tokio::test]
    async fn dispatcher_dispatch_with_empty_tags() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        dispatcher.dispatch_with_tags(&ctx, &[]).await.unwrap();
        let entries = dispatcher.history().get_all();
        assert!(entries[0].tags.is_empty());
    }

    #[tokio::test]
    async fn dispatcher_log_file_unicode_content() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_path = temp_dir.path().join("unicode.log");
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::LogFile {
                path: log_path.clone(),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };
        let dispatcher = NotificationDispatcher::new(config);
        let ctx = NotificationContext {
            task_id: "任务-001".into(),
            name: "中文文件.txt".into(),
            size: 2048,
            downloaded: 2048,
            protocol: "HTTP".into(),
            save_path: "/下载".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };
        dispatcher.dispatch(&ctx).await.unwrap();
        let content = tokio::fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("中文文件.txt"));
    }

    #[tokio::test]
    async fn dispatcher_log_file_appends() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_path = temp_dir.path().join("append.log");
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::LogFile {
                path: log_path.clone(),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };
        let dispatcher = NotificationDispatcher::new(config);
        for i in 0..3 {
            let ctx = make_test_ctx(
                &format!("t{}", i),
                "f.txt",
                NotificationEvent::DownloadComplete,
            );
            dispatcher.dispatch(&ctx).await.unwrap();
        }
        let content = tokio::fs::read_to_string(&log_path).await.unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn dispatcher_shell_command_unicode_template() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("unicode_out.txt");
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::Shell {
                command: format!("echo '{{name}}' > {}", output_path.display()),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };
        let dispatcher = NotificationDispatcher::new(config);
        let ctx = NotificationContext {
            task_id: "t1".into(),
            name: "日本語ファイル".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };
        dispatcher.dispatch(&ctx).await.unwrap();
        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert!(content.contains("日本語ファイル"));
    }

    // --- Complex workflows ---

    #[tokio::test]
    async fn workflow_subscribe_dispatch_unsubscribe() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let sub = dispatcher.subscribe(NotificationFilter::events(vec![
            NotificationEvent::DownloadComplete,
        ]));
        assert_eq!(dispatcher.subscription_count(), 1);
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        dispatcher.dispatch(&ctx).await.unwrap();
        assert!(dispatcher.unsubscribe(&sub.id));
        assert_eq!(dispatcher.subscription_count(), 0);
    }

    #[tokio::test]
    async fn workflow_multiple_events_history_order() {
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let events = [
            NotificationEvent::DownloadComplete,
            NotificationEvent::DownloadFailed,
            NotificationEvent::QueueEmpty,
        ];
        for (i, event) in events.iter().enumerate() {
            let ctx = make_test_ctx(&format!("t{}", i), "f.txt", *event);
            dispatcher.dispatch(&ctx).await.unwrap();
        }
        let entries = dispatcher.history().get_all();
        assert_eq!(entries.len(), 3);
        // Newest first
        assert_eq!(entries[0].event, NotificationEvent::QueueEmpty);
        assert_eq!(entries[1].event, NotificationEvent::DownloadFailed);
        assert_eq!(entries[2].event, NotificationEvent::DownloadComplete);
    }

    #[tokio::test]
    async fn workflow_config_update_then_dispatch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_path = temp_dir.path().join("update.log");
        let dispatcher = NotificationDispatcher::new(NotificationConfig::disabled());
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        // First dispatch: disabled, no channel output
        dispatcher.dispatch(&ctx).await.unwrap();
        // Update config to enable log file
        let config = NotificationConfig {
            enabled: true,
            channels: vec![NotificationChannel::LogFile {
                path: log_path.clone(),
            }],
            events: vec![NotificationEvent::DownloadComplete],
        };
        dispatcher.update_config(config);
        dispatcher.dispatch(&ctx).await.unwrap();
        let content = tokio::fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("download_complete"));
    }

    #[test]
    fn workflow_save_load_config_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = NotificationConfig {
            enabled: true,
            channels: vec![
                NotificationChannel::Desktop,
                NotificationChannel::Shell {
                    command: "echo done".into(),
                },
                NotificationChannel::LogFile {
                    path: PathBuf::from("/tmp/test.log"),
                },
            ],
            events: vec![
                NotificationEvent::DownloadComplete,
                NotificationEvent::DownloadFailed,
                NotificationEvent::QueueEmpty,
                NotificationEvent::ProgressMilestone,
            ],
        };
        save_notification_config(&config, temp_dir.path()).unwrap();
        let loaded = load_notification_config(temp_dir.path()).unwrap().unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.channels.len(), 3);
        assert_eq!(loaded.events.len(), 4);
    }

    // --- Edge cases ---

    #[test]
    fn filter_empty_events_matches_all() {
        let filter = NotificationFilter {
            events: vec![],
            task_ids: vec![],
            tags: vec![],
        };
        for event in [
            NotificationEvent::DownloadComplete,
            NotificationEvent::DownloadFailed,
            NotificationEvent::QueueEmpty,
            NotificationEvent::ProgressMilestone,
        ] {
            let ctx = make_test_ctx("t1", "f.txt", event);
            assert!(filter.matches(&ctx, &[]));
        }
    }

    #[test]
    fn filter_multiple_task_ids() {
        let ids: Vec<String> = (0..50).map(|i| format!("t{}", i)).collect();
        let filter = NotificationFilter::task_ids(ids.clone());
        for id in &ids {
            let ctx = make_test_ctx(id, "f.txt", NotificationEvent::DownloadComplete);
            assert!(filter.matches(&ctx, &[]));
        }
        let ctx = make_test_ctx("t999", "f.txt", NotificationEvent::DownloadComplete);
        assert!(!filter.matches(&ctx, &[]));
    }

    #[test]
    fn filter_multiple_tags() {
        let tags: Vec<String> = (0..20).map(|i| format!("tag{}", i)).collect();
        let filter = NotificationFilter {
            tags: tags.clone(),
            ..Default::default()
        };
        let ctx = make_test_ctx("t1", "f.txt", NotificationEvent::DownloadComplete);
        assert!(filter.matches(&ctx, &["tag19".into()]));
        assert!(!filter.matches(&ctx, &["other".into()]));
    }

    #[test]
    fn history_entry_large_size() {
        let entry = NotificationHistoryEntry {
            timestamp: chrono::Utc::now(),
            event: NotificationEvent::DownloadComplete,
            task_id: "t1".into(),
            name: "large.iso".into(),
            size: u64::MAX,
            downloaded: u64::MAX,
            protocol: "HTTP".into(),
            save_path: "/tmp".into(),
            error: None,
            tags: vec![],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let loaded: NotificationHistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.size, u64::MAX);
    }

    #[test]
    fn context_render_template_special_chars() {
        let ctx = NotificationContext {
            task_id: "t1".into(),
            name: "file with spaces & symbols < > \" ' ".into(),
            size: 100,
            downloaded: 100,
            protocol: "HTTP".into(),
            save_path: "/tmp/path".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };
        let rendered = ctx.render_template("{name}");
        assert_eq!(rendered, "file with spaces & symbols < > \" ' ");
    }
}
