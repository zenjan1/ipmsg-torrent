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
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::RwLock;
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
}

impl NotificationEvent {
    /// Human-readable label for templates
    pub fn label(&self) -> &'static str {
        match self {
            Self::DownloadComplete => "download_complete",
            Self::DownloadFailed => "download_failed",
            Self::QueueEmpty => "queue_empty",
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

/// Notification dispatcher
pub struct NotificationDispatcher {
    config: RwLock<NotificationConfig>,
    subscriptions: RwLock<HashMap<String, NotificationSubscription>>,
    event_sender: broadcast::Sender<NotificationContext>,
}

impl NotificationDispatcher {
    /// Create a new dispatcher with the given config
    pub fn new(config: NotificationConfig) -> Self {
        let (event_sender, _) = broadcast::channel(100);
        Self {
            config: RwLock::new(config),
            subscriptions: RwLock::new(HashMap::new()),
            event_sender,
        }
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
                    NotificationChannel::Shell { command } => {
                        self.send_shell(ctx, command).await
                    }
                    NotificationChannel::LogFile { path } => {
                        self.send_log_file(ctx, path).await
                    }
                    NotificationChannel::Webhook { url, .. } => {
                        self.send_webhook(ctx, url).await
                    }
                };

                if let Err(e) = result {
                    tracing::warn!(error = %e, channel = ?channel, "Failed to send notification");
                }
            }
        }

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

        let complete_ctx =
            make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
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
        let ctx_wrong_event =
            make_test_ctx("task1", "file.txt", NotificationEvent::DownloadFailed);
        assert!(!filter.matches(&ctx_wrong_event, &["important".into()]));

        // Wrong task ID
        let ctx_wrong_task =
            make_test_ctx("task2", "file.txt", NotificationEvent::DownloadComplete);
        assert!(!filter.matches(&ctx_wrong_task, &["important".into()]));

        // Wrong tag
        let ctx_wrong_tag =
            make_test_ctx("task1", "file.txt", NotificationEvent::DownloadComplete);
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
}
