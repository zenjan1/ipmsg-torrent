//! Download Notification Center
//!
//! Advanced notification management system that enhances the base notification module
//! with quiet hours, per-event preferences, batching, searchable history, and analytics.
//!
//! Features:
//! - Quiet hours: suppress notifications during configurable time windows
//! - Per-event channel preferences: route different events to different channels
//! - Notification batching: group multiple events into single notifications
//! - Rich history: searchable, filterable notification log with persistence
//! - Analytics: notification counts, delivery success rates, channel usage stats
//! - Priority-based routing: critical events bypass quiet hours

use chrono::{DateTime, Local, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, info};
use uuid::Uuid;

/// Errors from notification center operations
#[derive(Error, Debug)]
pub enum NotificationCenterError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid quiet hours configuration: {0}")]
    InvalidQuietHours(String),

    #[error("Invalid channel configuration: {0}")]
    InvalidChannel(String),
}

/// Notification priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NotificationPriority {
    /// Low priority (e.g., progress milestones)
    Low = 0,
    /// Normal priority (e.g., download complete)
    Normal = 1,
    /// High priority (e.g., download failed)
    High = 2,
    /// Critical priority (bypasses quiet hours)
    Critical = 3,
}

impl Default for NotificationPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Notification event types (extends base NotificationEvent)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NotificationCenterEvent {
    DownloadComplete,
    DownloadFailed,
    DownloadStarted,
    DownloadPaused,
    DownloadResumed,
    QueueEmpty,
    ProgressMilestone,
    SpeedAlert,
    DiskSpaceWarning,
    NetworkDisconnected,
}

impl NotificationCenterEvent {
    /// Get default priority for this event type
    pub fn default_priority(&self) -> NotificationPriority {
        match self {
            Self::DownloadComplete | Self::DownloadStarted | Self::DownloadResumed
            | Self::DownloadPaused => NotificationPriority::Normal,
            Self::ProgressMilestone => NotificationPriority::Low,
            Self::DownloadFailed | Self::SpeedAlert | Self::DiskSpaceWarning => {
                NotificationPriority::High
            }
            Self::QueueEmpty | Self::NetworkDisconnected => NotificationPriority::Critical,
        }
    }

    /// Human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::DownloadComplete => "download_complete",
            Self::DownloadFailed => "download_failed",
            Self::DownloadStarted => "download_started",
            Self::DownloadPaused => "download_paused",
            Self::DownloadResumed => "download_resumed",
            Self::QueueEmpty => "queue_empty",
            Self::ProgressMilestone => "progress_milestone",
            Self::SpeedAlert => "speed_alert",
            Self::DiskSpaceWarning => "disk_space_warning",
            Self::NetworkDisconnected => "network_disconnected",
        }
    }

    /// Create from base NotificationEvent
    pub fn from_base_event(event: &crate::notification::NotificationEvent) -> Self {
        match event {
            crate::notification::NotificationEvent::DownloadComplete => Self::DownloadComplete,
            crate::notification::NotificationEvent::DownloadFailed => Self::DownloadFailed,
            crate::notification::NotificationEvent::QueueEmpty => Self::QueueEmpty,
            crate::notification::NotificationEvent::ProgressMilestone => Self::ProgressMilestone,
        }
    }
}

/// Quiet hours configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuietHoursConfig {
    /// Whether quiet hours are enabled
    pub enabled: bool,
    /// Start time (local time)
    pub start_time: String,
    /// End time (local time)
    pub end_time: String,
    /// Whether critical notifications bypass quiet hours
    pub allow_critical: bool,
    /// Timezone (IANA format, e.g., "Asia/Shanghai")
    pub timezone: Option<String>,
}

impl Default for QuietHoursConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            start_time: "23:00".to_string(),
            end_time: "08:00".to_string(),
            allow_critical: true,
            timezone: None,
        }
    }
}

impl QuietHoursConfig {
    /// Check if a given time falls within quiet hours
    pub fn is_quiet_time(&self, now: DateTime<Local>) -> Result<bool, NotificationCenterError> {
        if !self.enabled {
            return Ok(false);
        }

        let start = NaiveTime::parse_from_str(&self.start_time, "%H:%M")
            .map_err(|e| NotificationCenterError::InvalidQuietHours(e.to_string()))?;
        let end = NaiveTime::parse_from_str(&self.end_time, "%H:%M")
            .map_err(|e| NotificationCenterError::InvalidQuietHours(e.to_string()))?;

        let current_time = now.time();

        // Handle overnight ranges (e.g., 23:00 - 08:00)
        if start > end {
            Ok(current_time >= start || current_time <= end)
        } else {
            Ok(current_time >= start && current_time <= end)
        }
    }
}

/// Per-event channel preference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventChannelPreference {
    /// Event type
    pub event: NotificationCenterEvent,
    /// Preferred channels for this event
    pub channels: Vec<String>,
    /// Override priority for this event
    pub priority_override: Option<NotificationPriority>,
    /// Whether this event is muted
    pub muted: bool,
}

/// Notification batching configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchingConfig {
    /// Whether batching is enabled
    pub enabled: bool,
    /// Batch window in seconds (group notifications within this time)
    pub window_secs: u64,
    /// Maximum batch size before forcing delivery
    pub max_batch_size: usize,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_secs: 60,
            max_batch_size: 10,
        }
    }
}

/// A pending notification in the batch queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingNotification {
    /// Unique ID
    pub id: String,
    /// Event type
    pub event: NotificationCenterEvent,
    /// Priority
    pub priority: NotificationPriority,
    /// Title
    pub title: String,
    /// Message body
    pub message: String,
    /// Timestamp when created
    pub created_at: DateTime<Utc>,
    /// Associated task ID (if any)
    pub task_id: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// A delivered notification record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationRecord {
    /// Unique ID
    pub id: String,
    /// Event type
    pub event: NotificationCenterEvent,
    /// Priority
    pub priority: NotificationPriority,
    /// Title
    pub title: String,
    /// Message body
    pub message: String,
    /// Channels it was sent to
    pub channels: Vec<String>,
    /// Timestamp when delivered
    pub delivered_at: DateTime<Utc>,
    /// Whether delivery was successful
    pub success: bool,
    /// Associated task ID (if any)
    pub task_id: Option<String>,
    /// Whether it was suppressed (quiet hours, muted, etc.)
    pub suppressed: bool,
    /// Suppression reason (if any)
    pub suppression_reason: Option<String>,
}

/// Notification channel statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChannelStats {
    /// Total notifications sent to this channel
    pub total_sent: u64,
    /// Successful deliveries
    pub successful: u64,
    /// Failed deliveries
    pub failed: u64,
    /// Average delivery time (ms)
    pub avg_delivery_ms: f64,
}

/// Notification analytics summary
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotificationAnalytics {
    /// Total notifications created
    pub total_created: u64,
    /// Total notifications delivered
    pub total_delivered: u64,
    /// Total notifications suppressed
    pub total_suppressed: u64,
    /// Per-channel statistics
    pub channel_stats: HashMap<String, ChannelStats>,
    /// Per-event type counts
    pub event_counts: HashMap<String, u64>,
    /// Notifications in last 24 hours
    pub last_24h_count: u64,
    /// Notifications in last 7 days
    pub last_7d_count: u64,
}

/// Notification center configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationCenterConfig {
    /// Quiet hours configuration
    pub quiet_hours: QuietHoursConfig,
    /// Batching configuration
    pub batching: BatchingConfig,
    /// Per-event channel preferences
    pub event_preferences: Vec<EventChannelPreference>,
    /// Maximum history size (oldest entries pruned)
    pub max_history_size: usize,
    /// Whether to persist history to disk
    pub persist_history: bool,
    /// History file path
    pub history_path: Option<PathBuf>,
}

impl Default for NotificationCenterConfig {
    fn default() -> Self {
        Self {
            quiet_hours: QuietHoursConfig::default(),
            batching: BatchingConfig::default(),
            event_preferences: Vec::new(),
            max_history_size: 1000,
            persist_history: true,
            history_path: None,
        }
    }
}

/// Notification center summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationCenterSummary {
    /// Current quiet hours status
    pub quiet_hours_active: bool,
    /// Pending batched notifications count
    pub pending_batch_count: usize,
    /// Total history size
    pub history_size: usize,
    /// Analytics summary
    pub analytics: NotificationAnalytics,
    /// Recent notifications (last 10)
    pub recent_notifications: Vec<NotificationRecord>,
}

/// Filter for querying notification history
#[derive(Debug, Clone, Default)]
pub struct NotificationFilter {
    /// Filter by event type
    pub event: Option<NotificationCenterEvent>,
    /// Filter by priority (minimum)
    pub min_priority: Option<NotificationPriority>,
    /// Filter by channel
    pub channel: Option<String>,
    /// Filter by task ID
    pub task_id: Option<String>,
    /// Filter by time range (start)
    pub start_time: Option<DateTime<Utc>>,
    /// Filter by time range (end)
    pub end_time: Option<DateTime<Utc>>,
    /// Filter by suppression status
    pub suppressed: Option<bool>,
    /// Maximum results to return
    pub limit: Option<usize>,
}

/// The notification center manager
pub struct NotificationCenterManager {
    config: NotificationCenterConfig,
    pending_batch: Vec<PendingNotification>,
    history: VecDeque<NotificationRecord>,
    analytics: NotificationAnalytics,
    last_batch_flush: DateTime<Utc>,
}

impl NotificationCenterManager {
    /// Create a new notification center manager
    pub fn new() -> Self {
        Self {
            config: NotificationCenterConfig::default(),
            pending_batch: Vec::new(),
            history: VecDeque::new(),
            analytics: NotificationAnalytics::default(),
            last_batch_flush: Utc::now(),
        }
    }

    /// Get current configuration
    pub fn get_config(&self) -> &NotificationCenterConfig {
        &self.config
    }

    /// Set configuration
    pub fn set_config(&mut self, config: NotificationCenterConfig) {
        self.config = config;
    }

    /// Check if quiet hours are currently active
    pub fn is_quiet_hours_active(&self) -> bool {
        let now = Local::now();
        self.config
            .quiet_hours
            .is_quiet_time(now)
            .unwrap_or(false)
    }

    /// Create and queue a notification
    pub fn notify(
        &mut self,
        event: NotificationCenterEvent,
        title: String,
        message: String,
        task_id: Option<String>,
        metadata: HashMap<String, String>,
    ) -> String {
        let priority = self.get_priority_for_event(event);
        let id = Uuid::new_v4().to_string();

        let notification = PendingNotification {
            id: id.clone(),
            event,
            priority,
            title,
            message,
            created_at: Utc::now(),
            task_id,
            metadata,
        };

        self.analytics.total_created += 1;

        // Check if should bypass batching (critical events or batching disabled)
        if !self.config.batching.enabled || priority == NotificationPriority::Critical {
            self.deliver_notification(notification);
        } else {
            self.pending_batch.push(notification);

            // Flush if batch size reached
            if self.pending_batch.len() >= self.config.batching.max_batch_size {
                self.flush_batch();
            }
        }

        id
    }

    /// Get priority for an event (considering preferences)
    fn get_priority_for_event(&self, event: NotificationCenterEvent) -> NotificationPriority {
        for pref in &self.config.event_preferences {
            if pref.event == event {
                return pref.priority_override.unwrap_or_else(|| event.default_priority());
            }
        }
        event.default_priority()
    }

    /// Check if an event is muted
    fn is_event_muted(&self, event: NotificationCenterEvent) -> bool {
        for pref in &self.config.event_preferences {
            if pref.event == event && pref.muted {
                return true;
            }
        }
        false
    }

    /// Get preferred channels for an event
    fn get_channels_for_event(&self, event: NotificationCenterEvent) -> Vec<String> {
        for pref in &self.config.event_preferences {
            if pref.event == event {
                return pref.channels.clone();
            }
        }
        // Default to all channels
        vec!["desktop".to_string(), "log".to_string()]
    }

    /// Deliver a notification (immediate or from batch)
    fn deliver_notification(&mut self, notification: PendingNotification) {
        let event = notification.event;
        let priority = notification.priority;

        // Check if muted
        if self.is_event_muted(event) {
            self.record_notification(notification, vec![], true, Some("muted".to_string()));
            return;
        }

        // Check quiet hours
        let is_quiet = self.is_quiet_hours_active();
        if is_quiet && priority < NotificationPriority::Critical {
            if !self.config.quiet_hours.allow_critical {
                self.record_notification(
                    notification,
                    vec![],
                    true,
                    Some("quiet_hours".to_string()),
                );
                return;
            }
        }

        // Get preferred channels
        let channels = self.get_channels_for_event(event);

        // Deliver to each channel (simulated - actual delivery would integrate with notification.rs)
        let mut success = true;
        for channel in &channels {
            // In real implementation, this would call the actual notification delivery
            debug!("Delivering notification to channel: {}", channel);
            self.update_channel_stats(channel, true, 0.0);
        }

        self.record_notification(notification, channels, !success, None);
    }

    /// Record a notification in history
    fn record_notification(
        &mut self,
        notification: PendingNotification,
        channels: Vec<String>,
        suppressed: bool,
        suppression_reason: Option<String>,
    ) {
        let record = NotificationRecord {
            id: notification.id,
            event: notification.event,
            priority: notification.priority,
            title: notification.title,
            message: notification.message,
            channels,
            delivered_at: Utc::now(),
            success: !suppressed,
            task_id: notification.task_id,
            suppressed,
            suppression_reason,
        };

        // Update analytics
        if suppressed {
            self.analytics.total_suppressed += 1;
        } else {
            self.analytics.total_delivered += 1;
        }

        let event_key = notification.event.label().to_string();
        *self.analytics.event_counts.entry(event_key).or_insert(0) += 1;

        // Add to history
        self.history.push_front(record);

        // Prune old entries
        while self.history.len() > self.config.max_history_size {
            self.history.pop_back();
        }
    }

    /// Update channel statistics
    fn update_channel_stats(&mut self, channel: &str, success: bool, delivery_ms: f64) {
        let stats = self
            .analytics
            .channel_stats
            .entry(channel.to_string())
            .or_default();
        stats.total_sent += 1;
        if success {
            stats.successful += 1;
        } else {
            stats.failed += 1;
        }
        // Update average delivery time (simple moving average)
        if stats.avg_delivery_ms == 0.0 {
            stats.avg_delivery_ms = delivery_ms;
        } else {
            stats.avg_delivery_ms = (stats.avg_delivery_ms + delivery_ms) / 2.0;
        }
    }

    /// Flush pending batched notifications
    pub fn flush_batch(&mut self) {
        let batch: Vec<PendingNotification> = self.pending_batch.drain(..).collect();
        for notification in batch {
            self.deliver_notification(notification);
        }
        self.last_batch_flush = Utc::now();
        debug!("Flushed notification batch");
    }

    /// Check and flush batch if window expired
    pub fn check_batch_timeout(&mut self) {
        if !self.config.batching.enabled || self.pending_batch.is_empty() {
            return;
        }

        let elapsed = Utc::now() - self.last_batch_flush;
        if elapsed.num_seconds() >= self.config.batching.window_secs as i64 {
            self.flush_batch();
        }
    }

    /// Get notification history with filtering
    pub fn get_history(&self, filter: NotificationFilter) -> Vec<NotificationRecord> {
        let mut results: Vec<&NotificationRecord> = self.history.iter().collect();

        // Apply filters
        if let Some(event) = filter.event {
            results.retain(|r| r.event == event);
        }

        if let Some(min_priority) = filter.min_priority {
            results.retain(|r| r.priority >= min_priority);
        }

        if let Some(ref channel) = filter.channel {
            results.retain(|r| r.channels.contains(channel));
        }

        if let Some(ref task_id) = filter.task_id {
            results.retain(|r| r.task_id.as_ref() == Some(task_id));
        }

        if let Some(start_time) = filter.start_time {
            results.retain(|r| r.delivered_at >= start_time);
        }

        if let Some(end_time) = filter.end_time {
            results.retain(|r| r.delivered_at <= end_time);
        }

        if let Some(suppressed) = filter.suppressed {
            results.retain(|r| r.suppressed == suppressed);
        }

        // Apply limit
        if let Some(limit) = filter.limit {
            results.truncate(limit);
        }

        results.into_iter().cloned().collect()
    }

    /// Get notification analytics
    pub fn get_analytics(&self) -> &NotificationAnalytics {
        &self.analytics
    }

    /// Get summary of notification center status
    pub fn get_summary(&self) -> NotificationCenterSummary {
        let recent: Vec<NotificationRecord> = self.history.iter().take(10).cloned().collect();

        NotificationCenterSummary {
            quiet_hours_active: self.is_quiet_hours_active(),
            pending_batch_count: self.pending_batch.len(),
            history_size: self.history.len(),
            analytics: self.analytics.clone(),
            recent_notifications: recent,
        }
    }

    /// Clear notification history
    pub fn clear_history(&mut self) {
        self.history.clear();
        info!("Cleared notification history");
    }

    /// Get pending batch count
    pub fn get_pending_batch_count(&self) -> usize {
        self.pending_batch.len()
    }

    /// Load configuration from file
    pub async fn load_config(
        path: &PathBuf,
    ) -> Result<NotificationCenterConfig, NotificationCenterError> {
        let content = tokio::fs::read_to_string(path).await?;
        let config: NotificationCenterConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to file
    pub async fn save_config(&self, path: &PathBuf) -> Result<(), NotificationCenterError> {
        let content = serde_json::to_string_pretty(&self.config)?;
        tokio::fs::write(path, content).await?;
        Ok(())
    }

    /// Load history from file
    pub async fn load_history(&mut self, path: &PathBuf) -> Result<(), NotificationCenterError> {
        if !path.exists() {
            return Ok(());
        }
        let content = tokio::fs::read_to_string(path).await?;
        let records: Vec<NotificationRecord> = serde_json::from_str(&content)?;
        self.history = records.into_iter().collect();
        Ok(())
    }

    /// Save history to file
    pub async fn save_history(&self, path: &PathBuf) -> Result<(), NotificationCenterError> {
        let records: Vec<&NotificationRecord> = self.history.iter().collect();
        let content = serde_json::to_string_pretty(&records)?;
        tokio::fs::write(path, content).await?;
        Ok(())
    }
}

impl Default for NotificationCenterManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_priority_ordering() {
        assert!(NotificationPriority::Critical > NotificationPriority::High);
        assert!(NotificationPriority::High > NotificationPriority::Normal);
        assert!(NotificationPriority::Normal > NotificationPriority::Low);
    }

    #[test]
    fn test_event_default_priority() {
        assert_eq!(
            NotificationCenterEvent::DownloadComplete.default_priority(),
            NotificationPriority::Normal
        );
        assert_eq!(
            NotificationCenterEvent::DownloadFailed.default_priority(),
            NotificationPriority::High
        );
        assert_eq!(
            NotificationCenterEvent::QueueEmpty.default_priority(),
            NotificationPriority::Critical
        );
    }

    #[test]
    fn test_quiet_hours_disabled() {
        let config = QuietHoursConfig::default();
        let now = Local::now();
        assert!(!config.is_quiet_time(now).unwrap());
    }

    #[test]
    fn test_quiet_hours_overnight() {
        let config = QuietHoursConfig {
            enabled: true,
            start_time: "23:00".to_string(),
            end_time: "08:00".to_string(),
            allow_critical: true,
            timezone: None,
        };

        // Test at 23:30 (should be quiet)
        let quiet_time = Local::now()
            .date_naive()
            .and_hms_opt(23, 30, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap();
        assert!(config.is_quiet_time(quiet_time).unwrap());

        // Test at 02:00 (should be quiet)
        let quiet_time2 = Local::now()
            .date_naive()
            .and_hms_opt(2, 0, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap();
        assert!(config.is_quiet_time(quiet_time2).unwrap());

        // Test at 10:00 (should not be quiet)
        let not_quiet = Local::now()
            .date_naive()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap();
        assert!(!config.is_quiet_time(not_quiet).unwrap());
    }

    #[test]
    fn test_notification_center_notify() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false; // Disable batching for immediate delivery

        let id = center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Download Complete".to_string(),
            "file.zip has finished downloading".to_string(),
            Some("task-123".to_string()),
            HashMap::new(),
        );

        assert!(!id.is_empty());
        assert_eq!(center.analytics.total_created, 1);
        assert_eq!(center.analytics.total_delivered, 1);
        assert_eq!(center.history.len(), 1);
    }

    #[test]
    fn test_notification_center_batching() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = true;
        center.config.batching.max_batch_size = 3;

        // Queue 2 notifications (should be batched)
        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Download 1".to_string(),
            "File 1".to_string(),
            None,
            HashMap::new(),
        );
        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Download 2".to_string(),
            "File 2".to_string(),
            None,
            HashMap::new(),
        );

        assert_eq!(center.get_pending_batch_count(), 2);
        assert_eq!(center.history.len(), 0); // Not delivered yet

        // Queue 3rd notification (should trigger flush)
        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Download 3".to_string(),
            "File 3".to_string(),
            None,
            HashMap::new(),
        );

        assert_eq!(center.get_pending_batch_count(), 0);
        assert_eq!(center.history.len(), 3); // All delivered
    }

    #[test]
    fn test_notification_center_critical_bypass_batch() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = true;

        // Critical notification should bypass batching
        center.notify(
            NotificationCenterEvent::QueueEmpty,
            "Queue Empty".to_string(),
            "All downloads finished".to_string(),
            None,
            HashMap::new(),
        );

        assert_eq!(center.get_pending_batch_count(), 0);
        assert_eq!(center.history.len(), 1);
    }

    #[test]
    fn test_notification_center_muted_event() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false;
        center.config.event_preferences.push(EventChannelPreference {
            event: NotificationCenterEvent::ProgressMilestone,
            channels: vec![],
            priority_override: None,
            muted: true,
        });

        center.notify(
            NotificationCenterEvent::ProgressMilestone,
            "Progress".to_string(),
            "50% complete".to_string(),
            None,
            HashMap::new(),
        );

        assert_eq!(center.analytics.total_suppressed, 1);
        assert_eq!(center.history.len(), 1);
        assert!(center.history[0].suppressed);
    }

    #[test]
    fn test_notification_history_filter() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false;

        // Add various notifications
        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Complete 1".to_string(),
            "Message 1".to_string(),
            Some("task-1".to_string()),
            HashMap::new(),
        );
        center.notify(
            NotificationCenterEvent::DownloadFailed,
            "Failed 1".to_string(),
            "Message 2".to_string(),
            Some("task-2".to_string()),
            HashMap::new(),
        );
        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Complete 2".to_string(),
            "Message 3".to_string(),
            Some("task-3".to_string()),
            HashMap::new(),
        );

        // Filter by event type
        let filter = NotificationFilter {
            event: Some(NotificationCenterEvent::DownloadComplete),
            ..Default::default()
        };
        let results = center.get_history(filter);
        assert_eq!(results.len(), 2);

        // Filter by task ID
        let filter = NotificationFilter {
            task_id: Some("task-2".to_string()),
            ..Default::default()
        };
        let results = center.get_history(filter);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event, NotificationCenterEvent::DownloadFailed);

        // Filter by priority
        let filter = NotificationFilter {
            min_priority: Some(NotificationPriority::High),
            ..Default::default()
        };
        let results = center.get_history(filter);
        assert_eq!(results.len(), 1);

        // Limit results
        let filter = NotificationFilter {
            limit: Some(2),
            ..Default::default()
        };
        let results = center.get_history(filter);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_notification_analytics() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false;

        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Complete".to_string(),
            "Message".to_string(),
            None,
            HashMap::new(),
        );
        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Complete 2".to_string(),
            "Message 2".to_string(),
            None,
            HashMap::new(),
        );
        center.notify(
            NotificationCenterEvent::DownloadFailed,
            "Failed".to_string(),
            "Message 3".to_string(),
            None,
            HashMap::new(),
        );

        let analytics = center.get_analytics();
        assert_eq!(analytics.total_created, 3);
        assert_eq!(analytics.total_delivered, 3);
        assert_eq!(*analytics.event_counts.get("download_complete").unwrap(), 2);
        assert_eq!(*analytics.event_counts.get("download_failed").unwrap(), 1);
    }

    #[test]
    fn test_notification_summary() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false;

        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Complete".to_string(),
            "Message".to_string(),
            None,
            HashMap::new(),
        );

        let summary = center.get_summary();
        assert_eq!(summary.history_size, 1);
        assert_eq!(summary.pending_batch_count, 0);
        assert_eq!(summary.recent_notifications.len(), 1);
    }

    #[test]
    fn test_clear_history() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false;

        center.notify(
            NotificationCenterEvent::DownloadComplete,
            "Complete".to_string(),
            "Message".to_string(),
            None,
            HashMap::new(),
        );

        assert_eq!(center.history.len(), 1);
        center.clear_history();
        assert_eq!(center.history.len(), 0);
    }

    #[test]
    fn test_history_max_size() {
        let mut center = NotificationCenterManager::new();
        center.config.batching.enabled = false;
        center.config.max_history_size = 5;

        // Add 10 notifications
        for i in 0..10 {
            center.notify(
                NotificationCenterEvent::DownloadComplete,
                format!("Complete {}", i),
                format!("Message {}", i),
                None,
                HashMap::new(),
            );
        }

        // Should only keep last 5
        assert_eq!(center.history.len(), 5);
    }

    #[test]
    fn test_event_channel_preference() {
        let mut center = NotificationCenterManager::new();
        center.config.event_preferences.push(EventChannelPreference {
            event: NotificationCenterEvent::DownloadFailed,
            channels: vec!["webhook".to_string(), "desktop".to_string()],
            priority_override: Some(NotificationPriority::Critical),
            muted: false,
        });

        // Check that preference is applied
        let priority = center.get_priority_for_event(NotificationCenterEvent::DownloadFailed);
        assert_eq!(priority, NotificationPriority::Critical);

        let channels = center.get_channels_for_event(NotificationCenterEvent::DownloadFailed);
        assert_eq!(channels, vec!["webhook", "desktop"]);
    }
}
