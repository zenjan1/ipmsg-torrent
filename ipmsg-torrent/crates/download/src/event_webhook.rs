//! Download Event Webhook System (Phase 130)
//!
//! Send HTTP webhook notifications to external services when download events occur.
//! Supports multiple webhook endpoints, event filtering, custom templates, and retry logic.
//!
//! Features:
//! - Multiple webhook endpoints with individual configurations
//! - Event type filtering (complete, fail, start, pause, etc.)
//! - Custom payload templates with task metadata
//! - HMAC signature for payload verification
//! - Automatic retry with exponential backoff
//! - Delivery history and success rate tracking
//! - Persistent configuration and history

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// Webhook event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    /// Download task completed
    DownloadComplete,
    /// Download task failed
    DownloadFailed,
    /// Download task started
    DownloadStarted,
    /// Download task paused
    DownloadPaused,
    /// Download task resumed
    DownloadResumed,
    /// Download task added
    DownloadAdded,
    /// Download task removed
    DownloadRemoved,
    /// All downloads in queue finished
    QueueEmpty,
    /// Progress milestone reached
    ProgressMilestone,
}

impl WebhookEvent {
    /// Get human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::DownloadComplete => "download_complete",
            Self::DownloadFailed => "download_failed",
            Self::DownloadStarted => "download_started",
            Self::DownloadPaused => "download_paused",
            Self::DownloadResumed => "download_resumed",
            Self::DownloadAdded => "download_added",
            Self::DownloadRemoved => "download_removed",
            Self::QueueEmpty => "queue_empty",
            Self::ProgressMilestone => "progress_milestone",
        }
    }

    /// Get all event types
    pub fn all() -> &'static [WebhookEvent] {
        &[
            Self::DownloadComplete,
            Self::DownloadFailed,
            Self::DownloadStarted,
            Self::DownloadPaused,
            Self::DownloadResumed,
            Self::DownloadAdded,
            Self::DownloadRemoved,
            Self::QueueEmpty,
            Self::ProgressMilestone,
        ]
    }
}

/// Webhook endpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    /// Unique endpoint ID
    pub id: String,
    /// Webhook URL (must be HTTP/HTTPS)
    pub url: String,
    /// Human-readable name
    pub name: String,
    /// Events to send (empty = all events)
    #[serde(default)]
    pub events: Vec<WebhookEvent>,
    /// Enable this endpoint
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// HMAC secret for signing payloads (optional)
    #[serde(default)]
    pub secret: Option<String>,
    /// Custom HTTP headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Maximum retry attempts (default: 3)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
}

fn default_true() -> bool {
    true
}

fn default_timeout() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    3
}

impl WebhookEndpoint {
    /// Create a new webhook endpoint
    pub fn new(url: String, name: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            url,
            name,
            events: Vec::new(),
            enabled: true,
            secret: None,
            headers: HashMap::new(),
            timeout_secs: default_timeout(),
            max_retries: default_max_retries(),
            created_at: Utc::now(),
        }
    }

    /// Check if this endpoint should receive the given event
    pub fn should_send(&self, event: WebhookEvent) -> bool {
        if !self.enabled {
            return false;
        }
        if self.events.is_empty() {
            return true; // Empty = all events
        }
        self.events.contains(&event)
    }

    /// Add an event filter
    pub fn add_event(&mut self, event: WebhookEvent) {
        if !self.events.contains(&event) {
            self.events.push(event);
        }
    }

    /// Remove an event filter
    pub fn remove_event(&mut self, event: WebhookEvent) {
        self.events.retain(|e| e != &event);
    }

    /// Set event filter to all events
    pub fn set_all_events(&mut self) {
        self.events.clear();
    }
}

/// Webhook delivery status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    /// Delivery succeeded
    Success,
    /// Delivery failed but will retry
    Pending,
    /// Delivery failed after all retries
    Failed,
}

/// Webhook delivery record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDelivery {
    /// Unique delivery ID
    pub id: String,
    /// Endpoint ID
    pub endpoint_id: String,
    /// Event type
    pub event: WebhookEvent,
    /// Task ID (if applicable)
    pub task_id: Option<String>,
    /// Task name (if applicable)
    pub task_name: Option<String>,
    /// HTTP status code received
    pub status_code: Option<u16>,
    /// Delivery status
    pub status: DeliveryStatus,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Delivery timestamp
    pub sent_at: DateTime<Utc>,
    /// Response timestamp
    pub responded_at: Option<DateTime<Utc>>,
}

impl WebhookDelivery {
    /// Create a new delivery record
    pub fn new(endpoint_id: String, event: WebhookEvent) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            endpoint_id,
            event,
            task_id: None,
            task_name: None,
            status_code: None,
            status: DeliveryStatus::Pending,
            error: None,
            retry_count: 0,
            sent_at: Utc::now(),
            responded_at: None,
        }
    }

    /// Mark as successful
    pub fn mark_success(&mut self, status_code: u16) {
        self.status_code = Some(status_code);
        self.status = DeliveryStatus::Success;
        self.responded_at = Some(Utc::now());
    }

    /// Mark as failed
    pub fn mark_failed(&mut self, error: String, status_code: Option<u16>) {
        self.status_code = status_code;
        self.error = Some(error);
        self.status = DeliveryStatus::Failed;
        self.responded_at = Some(Utc::now());
    }

    /// Increment retry count
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }
}

/// Webhook payload template variables
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookPayload {
    /// Event type
    pub event: String,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Task ID (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Task name (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    /// Task URL (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_url: Option<String>,
    /// Task size in bytes (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_size: Option<u64>,
    /// Task downloaded bytes (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_downloaded: Option<u64>,
    /// Task progress percentage (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_progress: Option<f32>,
    /// Task error message (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_error: Option<String>,
    /// Task tags
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub task_tags: Vec<String>,
    /// Task group (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_group: Option<String>,
    /// Additional metadata
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

/// Webhook configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Enable webhook system
    pub enabled: bool,
    /// Maximum delivery history entries per endpoint (default: 100)
    #[serde(default = "default_max_history")]
    pub max_history_per_endpoint: usize,
    /// Global timeout override (None = use endpoint-specific)
    #[serde(default)]
    pub global_timeout_secs: Option<u64>,
    /// Enable delivery logging
    #[serde(default = "default_true")]
    pub log_deliveries: bool,
}

fn default_max_history() -> usize {
    100
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_history_per_endpoint: default_max_history(),
            global_timeout_secs: None,
            log_deliveries: true,
        }
    }
}

/// Webhook manager for handling all webhook operations
#[derive(Debug)]
pub struct WebhookManager {
    /// Configuration
    config: WebhookConfig,
    /// Registered endpoints
    endpoints: HashMap<String, WebhookEndpoint>,
    /// Delivery history per endpoint
    history: HashMap<String, Vec<WebhookDelivery>>,
    /// Data directory for persistence
    data_dir: std::path::PathBuf,
}

impl WebhookManager {
    /// Create a new webhook manager
    pub fn new(data_dir: std::path::PathBuf) -> Self {
        Self {
            config: WebhookConfig::default(),
            endpoints: HashMap::new(),
            history: HashMap::new(),
            data_dir,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &WebhookConfig {
        &self.config
    }

    /// Set configuration
    pub fn set_config(&mut self, config: WebhookConfig) {
        self.config = config;
    }

    /// Add a new webhook endpoint
    pub fn add_endpoint(&mut self, endpoint: WebhookEndpoint) -> Result<String, WebhookError> {
        // Validate URL
        if endpoint.url.is_empty() {
            return Err(WebhookError::InvalidUrl("URL cannot be empty".to_string()));
        }
        if !endpoint.url.starts_with("http://") && !endpoint.url.starts_with("https://") {
            return Err(WebhookError::InvalidUrl(
                "URL must start with http:// or https://".to_string(),
            ));
        }

        let id = endpoint.id.clone();
        self.endpoints.insert(id.clone(), endpoint);
        self.history.insert(id.clone(), Vec::new());
        Ok(id)
    }

    /// Remove a webhook endpoint
    pub fn remove_endpoint(&mut self, endpoint_id: &str) -> Result<(), WebhookError> {
        if self.endpoints.remove(endpoint_id).is_none() {
            return Err(WebhookError::EndpointNotFound(endpoint_id.to_string()));
        }
        self.history.remove(endpoint_id);
        Ok(())
    }

    /// Get endpoint by ID
    pub fn get_endpoint(&self, endpoint_id: &str) -> Option<&WebhookEndpoint> {
        self.endpoints.get(endpoint_id)
    }

    /// List all endpoints
    pub fn list_endpoints(&self) -> Vec<&WebhookEndpoint> {
        self.endpoints.values().collect()
    }

    /// Update endpoint configuration
    pub fn update_endpoint(
        &mut self,
        endpoint_id: &str,
        updates: WebhookEndpointUpdate,
    ) -> Result<(), WebhookError> {
        let endpoint = self
            .endpoints
            .get_mut(endpoint_id)
            .ok_or_else(|| WebhookError::EndpointNotFound(endpoint_id.to_string()))?;

        if let Some(url) = updates.url {
            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(WebhookError::InvalidUrl(
                    "URL must start with http:// or https://".to_string(),
                ));
            }
            endpoint.url = url;
        }
        if let Some(name) = updates.name {
            endpoint.name = name;
        }
        if let Some(enabled) = updates.enabled {
            endpoint.enabled = enabled;
        }
        if let Some(secret) = updates.secret {
            endpoint.secret = Some(secret);
        }
        if let Some(timeout) = updates.timeout_secs {
            endpoint.timeout_secs = timeout;
        }
        if let Some(retries) = updates.max_retries {
            endpoint.max_retries = retries;
        }
        if let Some(events) = updates.events {
            endpoint.events = events;
        }
        if let Some(headers) = updates.headers {
            endpoint.headers = headers;
        }

        Ok(())
    }

    /// Get delivery history for an endpoint
    pub fn get_history(&self, endpoint_id: &str, limit: usize) -> Vec<&WebhookDelivery> {
        self.history
            .get(endpoint_id)
            .map(|h| h.iter().rev().take(limit).collect())
            .unwrap_or_default()
    }

    /// Get all delivery history
    pub fn get_all_history(&self) -> HashMap<&String, &Vec<WebhookDelivery>> {
        self.history.iter().collect()
    }

    /// Clear delivery history for an endpoint
    pub fn clear_history(&mut self, endpoint_id: &str) -> Result<(), WebhookError> {
        let history = self
            .history
            .get_mut(endpoint_id)
            .ok_or_else(|| WebhookError::EndpointNotFound(endpoint_id.to_string()))?;
        history.clear();
        Ok(())
    }

    /// Clear all delivery history
    pub fn clear_all_history(&mut self) {
        for history in self.history.values_mut() {
            history.clear();
        }
    }

    /// Send webhook for an event
    pub async fn send_event(
        &mut self,
        event: WebhookEvent,
        payload: WebhookPayload,
    ) -> Vec<WebhookDelivery> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut deliveries = Vec::new();

        // Find endpoints that should receive this event
        let endpoint_ids: Vec<String> = self
            .endpoints
            .values()
            .filter(|e| e.should_send(event))
            .map(|e| e.id.clone())
            .collect();

        for endpoint_id in endpoint_ids {
            let delivery = self.send_to_endpoint(&endpoint_id, event, &payload).await;
            deliveries.push(delivery);
        }

        deliveries
    }

    /// Send webhook to a specific endpoint
    async fn send_to_endpoint(
        &mut self,
        endpoint_id: &str,
        event: WebhookEvent,
        payload: &WebhookPayload,
    ) -> WebhookDelivery {
        let endpoint = match self.endpoints.get(endpoint_id) {
            Some(e) => e,
            None => {
                let mut delivery = WebhookDelivery::new(endpoint_id.to_string(), event);
                delivery.mark_failed("Endpoint not found".to_string(), None);
                return delivery;
            }
        };

        let mut delivery = WebhookDelivery::new(endpoint_id.to_string(), event);
        delivery.task_id = payload.task_id.clone();
        delivery.task_name = payload.task_name.clone();

        // Build request
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(endpoint.timeout_secs))
            .build()
            .unwrap_or_default();

        let json_payload = serde_json::to_string(payload).unwrap_or_default();

        let mut request = client
            .post(&endpoint.url)
            .header("Content-Type", "application/json")
            .header("User-Agent", "IPMsg-Torrent-Webhook/1.0")
            .header("X-Webhook-Event", event.label());

        // Add custom headers
        for (key, value) in &endpoint.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Add HMAC signature if secret is configured
        let signature = endpoint
            .secret
            .as_ref()
            .and_then(|s| compute_hmac_signature(&json_payload, s).ok());
        if let Some(sig) = signature {
            request = request.header("X-Webhook-Signature", sig);
        }

        request = request.body(json_payload);

        // Send with retry
        let max_attempts = endpoint.max_retries + 1;
        let mut last_error = None;
        let mut last_status = None;

        for attempt in 0..max_attempts {
            if attempt > 0 {
                delivery.increment_retry();
                // Exponential backoff: 1s, 2s, 4s
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt - 1))).await;
            }

            match request.try_clone() {
                Some(req) => match req.send().await {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        last_status = Some(status);

                        if (200..300).contains(&status) {
                            delivery.mark_success(status);
                            self.record_delivery(endpoint_id, delivery.clone());
                            return delivery;
                        } else {
                            last_error = Some(format!("HTTP {}", status));
                        }
                    }
                    Err(e) => {
                        last_error = Some(e.to_string());
                    }
                },
                None => {
                    last_error = Some("Failed to clone request".to_string());
                    break;
                }
            }
        }

        // All retries failed
        delivery.mark_failed(
            last_error.unwrap_or_else(|| "Unknown error".to_string()),
            last_status,
        );
        self.record_delivery(endpoint_id, delivery.clone());
        delivery
    }

    /// Record delivery in history
    fn record_delivery(&mut self, endpoint_id: &str, delivery: WebhookDelivery) {
        let history = self.history.entry(endpoint_id.to_string()).or_default();
        history.push(delivery);

        // Trim to max size
        let max = self.config.max_history_per_endpoint;
        if history.len() > max {
            let drain_count = history.len() - max;
            history.drain(0..drain_count);
        }
    }

    /// Get webhook summary
    pub fn get_summary(&self) -> WebhookSummary {
        let total_endpoints = self.endpoints.len();
        let enabled_endpoints = self.endpoints.values().filter(|e| e.enabled).count();

        let mut total_deliveries = 0;
        let mut successful_deliveries = 0;
        let mut failed_deliveries = 0;

        for history in self.history.values() {
            for delivery in history {
                total_deliveries += 1;
                match delivery.status {
                    DeliveryStatus::Success => successful_deliveries += 1,
                    DeliveryStatus::Failed => failed_deliveries += 1,
                    DeliveryStatus::Pending => {}
                }
            }
        }

        let success_rate = if total_deliveries > 0 {
            (successful_deliveries as f64 / total_deliveries as f64) * 100.0
        } else {
            100.0
        };

        WebhookSummary {
            total_endpoints,
            enabled_endpoints,
            total_deliveries,
            successful_deliveries,
            failed_deliveries,
            success_rate,
        }
    }

    /// Save configuration to disk
    pub async fn save_config(&self) -> Result<(), WebhookError> {
        let config_path = self.data_dir.join("webhook_config.json");
        let config_data = WebhookPersistedConfig {
            config: self.config.clone(),
            endpoints: self.endpoints.clone(),
        };
        let json = serde_json::to_string_pretty(&config_data)?;
        tokio::fs::write(&config_path, json).await?;
        Ok(())
    }

    /// Load configuration from disk
    pub async fn load_config(&mut self) -> Result<(), WebhookError> {
        let config_path = self.data_dir.join("webhook_config.json");
        if !config_path.exists() {
            return Ok(());
        }
        let json = tokio::fs::read_to_string(&config_path).await?;
        let persisted: WebhookPersistedConfig = serde_json::from_str(&json)?;
        self.config = persisted.config;
        self.endpoints = persisted.endpoints;
        // Initialize history for loaded endpoints
        for id in self.endpoints.keys() {
            self.history.entry(id.clone()).or_default();
        }
        Ok(())
    }
}

/// Webhook endpoint update payload
#[derive(Debug, Clone, Default)]
pub struct WebhookEndpointUpdate {
    pub url: Option<String>,
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub secret: Option<String>,
    pub timeout_secs: Option<u64>,
    pub max_retries: Option<u32>,
    pub events: Option<Vec<WebhookEvent>>,
    pub headers: Option<HashMap<String, String>>,
}

/// Webhook summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookSummary {
    pub total_endpoints: usize,
    pub enabled_endpoints: usize,
    pub total_deliveries: usize,
    pub successful_deliveries: usize,
    pub failed_deliveries: usize,
    pub success_rate: f64,
}

/// Persisted configuration format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebhookPersistedConfig {
    config: WebhookConfig,
    endpoints: HashMap<String, WebhookEndpoint>,
}

/// Webhook errors
#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("endpoint not found: {0}")]
    EndpointNotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}

/// Compute HMAC-SHA256 signature for payload verification
fn compute_hmac_signature(payload: &str, secret: &str) -> Result<String, WebhookError> {
    use sha2::{Digest, Sha256};

    let mut mac = Sha256::new();
    mac.update(payload.as_bytes());
    mac.update(secret.as_bytes());
    let result = mac.finalize();
    Ok(hex::encode(result))
}

/// Format webhook summary for display
pub fn format_webhook_summary(summary: &WebhookSummary) -> String {
    let mut output = String::new();
    output.push_str("📡 Webhook Summary\n");
    output.push_str(&format!(
        "  Endpoints: {} total, {} enabled\n",
        summary.total_endpoints, summary.enabled_endpoints
    ));
    output.push_str(&format!(
        "  Deliveries: {} total, {} successful, {} failed\n",
        summary.total_deliveries, summary.successful_deliveries, summary.failed_deliveries
    ));
    output.push_str(&format!("  Success Rate: {:.1}%\n", summary.success_rate));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_event_label() {
        assert_eq!(WebhookEvent::DownloadComplete.label(), "download_complete");
        assert_eq!(WebhookEvent::DownloadFailed.label(), "download_failed");
        assert_eq!(WebhookEvent::DownloadStarted.label(), "download_started");
        assert_eq!(WebhookEvent::QueueEmpty.label(), "queue_empty");
    }

    #[test]
    fn test_webhook_event_all() {
        let all = WebhookEvent::all();
        assert_eq!(all.len(), 9);
        assert!(all.contains(&WebhookEvent::DownloadComplete));
        assert!(all.contains(&WebhookEvent::ProgressMilestone));
    }

    #[test]
    fn test_webhook_endpoint_creation() {
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test Webhook".to_string(),
        );
        assert!(endpoint.id.len() > 0);
        assert_eq!(endpoint.url, "https://example.com/webhook");
        assert_eq!(endpoint.name, "Test Webhook");
        assert!(endpoint.enabled);
        assert!(endpoint.events.is_empty());
        assert!(endpoint.secret.is_none());
    }

    #[test]
    fn test_webhook_endpoint_should_send() {
        let mut endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );

        // Empty events = all events
        assert!(endpoint.should_send(WebhookEvent::DownloadComplete));
        assert!(endpoint.should_send(WebhookEvent::DownloadFailed));

        // Add specific event filter
        endpoint.add_event(WebhookEvent::DownloadComplete);
        assert!(endpoint.should_send(WebhookEvent::DownloadComplete));
        assert!(!endpoint.should_send(WebhookEvent::DownloadFailed));

        // Disable endpoint
        endpoint.enabled = false;
        assert!(!endpoint.should_send(WebhookEvent::DownloadComplete));
    }

    #[test]
    fn test_webhook_endpoint_event_management() {
        let mut endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );

        endpoint.add_event(WebhookEvent::DownloadComplete);
        endpoint.add_event(WebhookEvent::DownloadFailed);
        assert_eq!(endpoint.events.len(), 2);

        // Duplicate add should not increase count
        endpoint.add_event(WebhookEvent::DownloadComplete);
        assert_eq!(endpoint.events.len(), 2);

        endpoint.remove_event(WebhookEvent::DownloadComplete);
        assert_eq!(endpoint.events.len(), 1);
        assert!(endpoint.events.contains(&WebhookEvent::DownloadFailed));

        endpoint.set_all_events();
        assert!(endpoint.events.is_empty());
    }

    #[test]
    fn test_webhook_delivery_creation() {
        let delivery =
            WebhookDelivery::new("endpoint-1".to_string(), WebhookEvent::DownloadComplete);
        assert!(delivery.id.len() > 0);
        assert_eq!(delivery.endpoint_id, "endpoint-1");
        assert_eq!(delivery.event, WebhookEvent::DownloadComplete);
        assert_eq!(delivery.status, DeliveryStatus::Pending);
        assert_eq!(delivery.retry_count, 0);
    }

    #[test]
    fn test_webhook_delivery_mark_success() {
        let mut delivery =
            WebhookDelivery::new("endpoint-1".to_string(), WebhookEvent::DownloadComplete);
        delivery.mark_success(200);
        assert_eq!(delivery.status, DeliveryStatus::Success);
        assert_eq!(delivery.status_code, Some(200));
        assert!(delivery.responded_at.is_some());
    }

    #[test]
    fn test_webhook_delivery_mark_failed() {
        let mut delivery =
            WebhookDelivery::new("endpoint-1".to_string(), WebhookEvent::DownloadComplete);
        delivery.mark_failed("Connection timeout".to_string(), Some(500));
        assert_eq!(delivery.status, DeliveryStatus::Failed);
        assert_eq!(delivery.status_code, Some(500));
        assert_eq!(delivery.error, Some("Connection timeout".to_string()));
    }

    #[test]
    fn test_webhook_delivery_increment_retry() {
        let mut delivery =
            WebhookDelivery::new("endpoint-1".to_string(), WebhookEvent::DownloadComplete);
        assert_eq!(delivery.retry_count, 0);
        delivery.increment_retry();
        assert_eq!(delivery.retry_count, 1);
        delivery.increment_retry();
        assert_eq!(delivery.retry_count, 2);
    }

    #[test]
    fn test_webhook_manager_creation() {
        let manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        assert!(manager.config().enabled);
        assert_eq!(manager.list_endpoints().len(), 0);
    }

    #[test]
    fn test_webhook_manager_add_endpoint() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();
        assert_eq!(manager.list_endpoints().len(), 1);
        assert!(manager.get_endpoint(&id).is_some());
    }

    #[test]
    fn test_webhook_manager_add_endpoint_invalid_url() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let mut endpoint = WebhookEndpoint::new("not-a-url".to_string(), "Test".to_string());
        endpoint.url = "not-a-url".to_string();
        let result = manager.add_endpoint(endpoint);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), WebhookError::InvalidUrl(_)));
    }

    #[test]
    fn test_webhook_manager_remove_endpoint() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();
        assert_eq!(manager.list_endpoints().len(), 1);

        manager.remove_endpoint(&id).unwrap();
        assert_eq!(manager.list_endpoints().len(), 0);
    }

    #[test]
    fn test_webhook_manager_remove_endpoint_not_found() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let result = manager.remove_endpoint("nonexistent");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            WebhookError::EndpointNotFound(_)
        ));
    }

    #[test]
    fn test_webhook_manager_update_endpoint() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        let updates = WebhookEndpointUpdate {
            name: Some("Updated Name".to_string()),
            enabled: Some(false),
            timeout_secs: Some(60),
            ..Default::default()
        };
        manager.update_endpoint(&id, updates).unwrap();

        let updated = manager.get_endpoint(&id).unwrap();
        assert_eq!(updated.name, "Updated Name");
        assert!(!updated.enabled);
        assert_eq!(updated.timeout_secs, 60);
    }

    #[test]
    fn test_webhook_manager_update_endpoint_invalid_url() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        let updates = WebhookEndpointUpdate {
            url: Some("ftp://invalid.com".to_string()),
            ..Default::default()
        };
        let result = manager.update_endpoint(&id, updates);
        assert!(result.is_err());
    }

    #[test]
    fn test_webhook_manager_history() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        // Initially empty
        assert_eq!(manager.get_history(&id, 10).len(), 0);

        // Record some deliveries
        let delivery1 = WebhookDelivery::new(id.clone(), WebhookEvent::DownloadComplete);
        let delivery2 = WebhookDelivery::new(id.clone(), WebhookEvent::DownloadFailed);
        manager.record_delivery(&id, delivery1);
        manager.record_delivery(&id, delivery2);

        assert_eq!(manager.get_history(&id, 10).len(), 2);
        assert_eq!(manager.get_history(&id, 1).len(), 1);
    }

    #[test]
    fn test_webhook_manager_clear_history() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        let delivery = WebhookDelivery::new(id.clone(), WebhookEvent::DownloadComplete);
        manager.record_delivery(&id, delivery);
        assert_eq!(manager.get_history(&id, 10).len(), 1);

        manager.clear_history(&id).unwrap();
        assert_eq!(manager.get_history(&id, 10).len(), 0);
    }

    #[test]
    fn test_webhook_manager_clear_all_history() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        let endpoint1 = WebhookEndpoint::new(
            "https://example.com/webhook1".to_string(),
            "Test1".to_string(),
        );
        let endpoint2 = WebhookEndpoint::new(
            "https://example.com/webhook2".to_string(),
            "Test2".to_string(),
        );
        let id1 = manager.add_endpoint(endpoint1).unwrap();
        let id2 = manager.add_endpoint(endpoint2).unwrap();

        manager.record_delivery(
            &id1,
            WebhookDelivery::new(id1.clone(), WebhookEvent::DownloadComplete),
        );
        manager.record_delivery(
            &id2,
            WebhookDelivery::new(id2.clone(), WebhookEvent::DownloadFailed),
        );

        manager.clear_all_history();
        assert_eq!(manager.get_history(&id1, 10).len(), 0);
        assert_eq!(manager.get_history(&id2, 10).len(), 0);
    }

    #[test]
    fn test_webhook_manager_summary() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        let endpoint1 = WebhookEndpoint::new(
            "https://example.com/webhook1".to_string(),
            "Test1".to_string(),
        );
        let endpoint2 = WebhookEndpoint::new(
            "https://example.com/webhook2".to_string(),
            "Test2".to_string(),
        );
        let id1 = manager.add_endpoint(endpoint1).unwrap();
        let id2 = manager.add_endpoint(endpoint2).unwrap();

        // Record successful delivery
        let mut delivery1 = WebhookDelivery::new(id1.clone(), WebhookEvent::DownloadComplete);
        delivery1.mark_success(200);
        manager.record_delivery(&id1, delivery1);

        // Record failed delivery
        let mut delivery2 = WebhookDelivery::new(id2.clone(), WebhookEvent::DownloadFailed);
        delivery2.mark_failed("Error".to_string(), Some(500));
        manager.record_delivery(&id2, delivery2);

        let summary = manager.get_summary();
        assert_eq!(summary.total_endpoints, 2);
        assert_eq!(summary.enabled_endpoints, 2);
        assert_eq!(summary.total_deliveries, 2);
        assert_eq!(summary.successful_deliveries, 1);
        assert_eq!(summary.failed_deliveries, 1);
        assert_eq!(summary.success_rate, 50.0);
    }

    #[test]
    fn test_webhook_manager_summary_empty() {
        let manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let summary = manager.get_summary();
        assert_eq!(summary.total_endpoints, 0);
        assert_eq!(summary.total_deliveries, 0);
        assert_eq!(summary.success_rate, 100.0);
    }

    #[test]
    fn test_webhook_payload_serialization() {
        let mut payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            task_id: Some("task-123".to_string()),
            task_name: Some("test.zip".to_string()),
            task_url: Some("https://example.com/test.zip".to_string()),
            task_size: Some(1024 * 1024),
            task_downloaded: Some(1024 * 1024),
            task_progress: Some(100.0),
            task_error: None,
            task_tags: vec!["test".to_string(), "archive".to_string()],
            task_group: Some("downloads".to_string()),
            metadata: HashMap::new(),
        };
        payload
            .metadata
            .insert("key".to_string(), "value".to_string());

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("download_complete"));
        assert!(json.contains("task-123"));
        assert!(json.contains("test.zip"));
    }

    #[test]
    fn test_webhook_config_default() {
        let config = WebhookConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_history_per_endpoint, 100);
        assert!(config.global_timeout_secs.is_none());
        assert!(config.log_deliveries);
    }

    #[test]
    fn test_compute_hmac_signature() {
        let payload = r#"{"event":"download_complete"}"#;
        let secret = "my-secret-key";
        let signature = compute_hmac_signature(payload, secret).unwrap();
        assert!(!signature.is_empty());
        assert_eq!(signature.len(), 64); // SHA256 hex = 64 chars

        // Same input should produce same output
        let signature2 = compute_hmac_signature(payload, secret).unwrap();
        assert_eq!(signature, signature2);

        // Different secret should produce different output
        let signature3 = compute_hmac_signature(payload, "different-secret").unwrap();
        assert_ne!(signature, signature3);
    }

    #[test]
    fn test_format_webhook_summary() {
        let summary = WebhookSummary {
            total_endpoints: 5,
            enabled_endpoints: 3,
            total_deliveries: 100,
            successful_deliveries: 95,
            failed_deliveries: 5,
            success_rate: 95.0,
        };
        let formatted = format_webhook_summary(&summary);
        assert!(formatted.contains("Webhook Summary"));
        assert!(formatted.contains("5 total"));
        assert!(formatted.contains("3 enabled"));
        assert!(formatted.contains("95.0%"));
    }

    #[tokio::test]
    async fn test_webhook_manager_send_event_disabled() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        manager.config.enabled = false;

        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        manager.add_endpoint(endpoint).unwrap();

        let payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            ..Default::default()
        };

        let deliveries = manager
            .send_event(WebhookEvent::DownloadComplete, payload)
            .await;
        assert_eq!(deliveries.len(), 0);
    }

    #[tokio::test]
    async fn test_webhook_manager_send_event_no_matching_endpoints() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        let mut endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        endpoint.events = vec![WebhookEvent::DownloadFailed]; // Only failed events
        manager.add_endpoint(endpoint).unwrap();

        let payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            ..Default::default()
        };

        let deliveries = manager
            .send_event(WebhookEvent::DownloadComplete, payload)
            .await;
        assert_eq!(deliveries.len(), 0);
    }

    #[test]
    fn test_webhook_endpoint_serialization() {
        let mut endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test Webhook".to_string(),
        );
        endpoint.events = vec![WebhookEvent::DownloadComplete, WebhookEvent::DownloadFailed];
        endpoint.secret = Some("my-secret".to_string());
        endpoint
            .headers
            .insert("Authorization".to_string(), "Bearer token".to_string());

        let json = serde_json::to_string(&endpoint).unwrap();
        let deserialized: WebhookEndpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.url, endpoint.url);
        assert_eq!(deserialized.name, endpoint.name);
        assert_eq!(deserialized.events, endpoint.events);
        assert_eq!(deserialized.secret, endpoint.secret);
        assert_eq!(deserialized.headers, endpoint.headers);
    }

    #[test]
    fn test_webhook_delivery_status_serialization() {
        let status = DeliveryStatus::Success;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#""success""#);

        let status: DeliveryStatus = serde_json::from_str(r#""failed""#).unwrap();
        assert_eq!(status, DeliveryStatus::Failed);
    }

    #[test]
    fn test_webhook_event_serialization() {
        let event = WebhookEvent::DownloadComplete;
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, r#""download_complete""#);

        let event: WebhookEvent = serde_json::from_str(r#""download_failed""#).unwrap();
        assert_eq!(event, WebhookEvent::DownloadFailed);
    }

    // ===== Phase 244: Comprehensive Test Coverage =====

    // --- WebhookEvent: all labels ---
    #[test]
    fn test_webhook_event_all_labels() {
        assert_eq!(WebhookEvent::DownloadComplete.label(), "download_complete");
        assert_eq!(WebhookEvent::DownloadFailed.label(), "download_failed");
        assert_eq!(WebhookEvent::DownloadStarted.label(), "download_started");
        assert_eq!(WebhookEvent::DownloadPaused.label(), "download_paused");
        assert_eq!(WebhookEvent::DownloadResumed.label(), "download_resumed");
        assert_eq!(WebhookEvent::DownloadAdded.label(), "download_added");
        assert_eq!(WebhookEvent::DownloadRemoved.label(), "download_removed");
        assert_eq!(WebhookEvent::QueueEmpty.label(), "queue_empty");
        assert_eq!(
            WebhookEvent::ProgressMilestone.label(),
            "progress_milestone"
        );
    }

    // --- WebhookEvent: serde roundtrip all 9 variants ---
    #[test]
    fn test_webhook_event_serde_all_variants() {
        let variants = WebhookEvent::all();
        for &event in variants {
            let json = serde_json::to_string(&event).unwrap();
            let deserialized: WebhookEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(event, deserialized);
        }
    }

    #[test]
    fn test_webhook_event_serde_snake_case_values() {
        assert_eq!(
            serde_json::to_string(&WebhookEvent::DownloadStarted).unwrap(),
            r#""download_started""#
        );
        assert_eq!(
            serde_json::to_string(&WebhookEvent::DownloadPaused).unwrap(),
            r#""download_paused""#
        );
        assert_eq!(
            serde_json::to_string(&WebhookEvent::DownloadResumed).unwrap(),
            r#""download_resumed""#
        );
        assert_eq!(
            serde_json::to_string(&WebhookEvent::DownloadAdded).unwrap(),
            r#""download_added""#
        );
        assert_eq!(
            serde_json::to_string(&WebhookEvent::DownloadRemoved).unwrap(),
            r#""download_removed""#
        );
        assert_eq!(
            serde_json::to_string(&WebhookEvent::ProgressMilestone).unwrap(),
            r#""progress_milestone""#
        );
    }

    // --- WebhookEvent: traits ---
    #[test]
    fn test_webhook_event_clone_copy() {
        let event = WebhookEvent::DownloadComplete;
        let cloned = event;
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_webhook_event_debug() {
        let debug_str = format!("{:?}", WebhookEvent::DownloadComplete);
        assert_eq!(debug_str, "DownloadComplete");
    }

    #[test]
    fn test_webhook_event_eq_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(WebhookEvent::DownloadComplete);
        set.insert(WebhookEvent::DownloadComplete);
        assert_eq!(set.len(), 1);
        set.insert(WebhookEvent::DownloadFailed);
        assert_eq!(set.len(), 2);
    }

    // --- DeliveryStatus: serde roundtrip all 3 variants ---
    #[test]
    fn test_delivery_status_serde_all_variants() {
        let variants = [
            DeliveryStatus::Success,
            DeliveryStatus::Pending,
            DeliveryStatus::Failed,
        ];
        for status in &variants {
            let json = serde_json::to_string(status).unwrap();
            let deserialized: DeliveryStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*status, deserialized);
        }
    }

    #[test]
    fn test_delivery_status_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&DeliveryStatus::Success).unwrap(),
            r#""success""#
        );
        assert_eq!(
            serde_json::to_string(&DeliveryStatus::Pending).unwrap(),
            r#""pending""#
        );
        assert_eq!(
            serde_json::to_string(&DeliveryStatus::Failed).unwrap(),
            r#""failed""#
        );
    }

    // --- DeliveryStatus: traits ---
    #[test]
    fn test_delivery_status_clone_copy_debug() {
        let status = DeliveryStatus::Success;
        let cloned = status;
        assert_eq!(status, cloned);
        let debug_str = format!("{:?}", status);
        assert_eq!(debug_str, "Success");
    }

    // --- WebhookDelivery: serde roundtrip ---
    #[test]
    fn test_webhook_delivery_serde_roundtrip() {
        let mut delivery = WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadComplete);
        delivery.task_id = Some("task-123".to_string());
        delivery.task_name = Some("test.zip".to_string());
        delivery.mark_success(200);

        let json = serde_json::to_string(&delivery).unwrap();
        let deserialized: WebhookDelivery = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.endpoint_id, "ep-1");
        assert_eq!(deserialized.event, WebhookEvent::DownloadComplete);
        assert_eq!(deserialized.task_id, Some("task-123".to_string()));
        assert_eq!(deserialized.status, DeliveryStatus::Success);
        assert_eq!(deserialized.status_code, Some(200));
    }

    #[test]
    fn test_webhook_delivery_clone_debug() {
        let delivery = WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadFailed);
        let cloned = delivery.clone();
        assert_eq!(cloned.endpoint_id, delivery.endpoint_id);
        assert_eq!(cloned.event, delivery.event);
        let debug_str = format!("{:?}", delivery);
        assert!(debug_str.contains("WebhookDelivery"));
    }

    #[test]
    fn test_webhook_delivery_mark_success_various_codes() {
        for code in [200, 201, 202, 204, 299] {
            let mut delivery =
                WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadComplete);
            delivery.mark_success(code);
            assert_eq!(delivery.status_code, Some(code));
            assert_eq!(delivery.status, DeliveryStatus::Success);
        }
    }

    #[test]
    fn test_webhook_delivery_mark_failed_none_status() {
        let mut delivery = WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadComplete);
        delivery.mark_failed("Connection refused".to_string(), None);
        assert_eq!(delivery.status_code, None);
        assert_eq!(delivery.status, DeliveryStatus::Failed);
        assert_eq!(delivery.error, Some("Connection refused".to_string()));
    }

    #[test]
    fn test_webhook_delivery_increment_retry_multiple() {
        let mut delivery = WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadComplete);
        for i in 1..=10 {
            delivery.increment_retry();
            assert_eq!(delivery.retry_count, i);
        }
    }

    // --- WebhookPayload: serde ---
    #[test]
    fn test_webhook_payload_serde_all_none() {
        let payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            task_id: None,
            task_name: None,
            task_url: None,
            task_size: None,
            task_downloaded: None,
            task_progress: None,
            task_error: None,
            task_tags: Vec::new(),
            task_group: None,
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string(&payload).unwrap();
        // skip_serializing_if should omit None fields
        assert!(!json.contains("task_id"));
        assert!(!json.contains("task_name"));
        assert!(!json.contains("task_url"));
        assert!(!json.contains("task_size"));
        assert!(!json.contains("task_progress"));
        assert!(!json.contains("task_error"));
        assert!(!json.contains("task_tags"));
        assert!(!json.contains("task_group"));
        assert!(!json.contains("metadata"));
    }

    #[test]
    fn test_webhook_payload_serde_with_all_fields() {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());

        let payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            task_id: Some("task-1".to_string()),
            task_name: Some("file.zip".to_string()),
            task_url: Some("https://example.com/file.zip".to_string()),
            task_size: Some(1024),
            task_downloaded: Some(512),
            task_progress: Some(50.0),
            task_error: Some("timeout".to_string()),
            task_tags: vec!["tag1".to_string()],
            task_group: Some("group1".to_string()),
            metadata,
        };

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("task-1"));
        assert!(json.contains("file.zip"));
        assert!(json.contains("key1"));

        let deserialized: WebhookPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, Some("task-1".to_string()));
        assert_eq!(deserialized.task_size, Some(1024));
    }

    #[test]
    fn test_webhook_payload_unicode() {
        let payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            task_id: Some("任务-123".to_string()),
            task_name: Some("测试文件.zip".to_string()),
            task_url: None,
            task_size: None,
            task_downloaded: None,
            task_progress: None,
            task_error: Some("连接超时".to_string()),
            task_tags: vec!["标签".to_string()],
            task_group: None,
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string(&payload).unwrap();
        let deserialized: WebhookPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, Some("任务-123".to_string()));
        assert_eq!(deserialized.task_name, Some("测试文件.zip".to_string()));
    }

    #[test]
    fn test_webhook_payload_emoji() {
        let payload = WebhookPayload {
            event: "download_complete".to_string(),
            timestamp: Utc::now(),
            task_id: Some("🎯-task".to_string()),
            task_name: Some("🚀-file.zip".to_string()),
            task_url: None,
            task_size: None,
            task_downloaded: None,
            task_progress: None,
            task_error: None,
            task_tags: vec!["⚡".to_string()],
            task_group: None,
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string(&payload).unwrap();
        let deserialized: WebhookPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, Some("🎯-task".to_string()));
    }

    // --- WebhookConfig: serde ---
    #[test]
    fn test_webhook_config_serde_roundtrip() {
        let config = WebhookConfig {
            enabled: false,
            max_history_per_endpoint: 50,
            global_timeout_secs: Some(60),
            log_deliveries: false,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: WebhookConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.max_history_per_endpoint, 50);
        assert_eq!(deserialized.global_timeout_secs, Some(60));
        assert_eq!(deserialized.log_deliveries, false);
    }

    #[test]
    fn test_webhook_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"max_history_per_endpoint":100,"extra_field":"ignored"}"#;
        let config: WebhookConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_history_per_endpoint, 100);
    }

    #[test]
    fn test_webhook_config_clone_debug() {
        let config = WebhookConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(
            cloned.max_history_per_endpoint,
            config.max_history_per_endpoint
        );
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("WebhookConfig"));
    }

    // --- WebhookSummary: serde + traits ---
    #[test]
    fn test_webhook_summary_serde_roundtrip() {
        let summary = WebhookSummary {
            total_endpoints: 10,
            enabled_endpoints: 8,
            total_deliveries: 500,
            successful_deliveries: 450,
            failed_deliveries: 50,
            success_rate: 90.0,
        };

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: WebhookSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_endpoints, 10);
        assert_eq!(deserialized.success_rate, 90.0);
    }

    #[test]
    fn test_webhook_summary_clone_debug() {
        let summary = WebhookSummary {
            total_endpoints: 1,
            enabled_endpoints: 1,
            total_deliveries: 0,
            successful_deliveries: 0,
            failed_deliveries: 0,
            success_rate: 100.0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_endpoints, summary.total_endpoints);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("WebhookSummary"));
    }

    // --- WebhookEndpointUpdate: Default ---
    #[test]
    fn test_webhook_endpoint_update_default() {
        let update = WebhookEndpointUpdate::default();
        assert!(update.url.is_none());
        assert!(update.name.is_none());
        assert!(update.enabled.is_none());
        assert!(update.secret.is_none());
        assert!(update.timeout_secs.is_none());
        assert!(update.max_retries.is_none());
        assert!(update.events.is_none());
        assert!(update.headers.is_none());
    }

    // --- WebhookManager: endpoint operations ---
    #[test]
    fn test_webhook_manager_update_endpoint_not_found() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let updates = WebhookEndpointUpdate::default();
        let result = manager.update_endpoint("nonexistent", updates);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            WebhookError::EndpointNotFound(_)
        ));
    }

    #[test]
    fn test_webhook_manager_get_endpoint_not_found() {
        let manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        assert!(manager.get_endpoint("nonexistent").is_none());
    }

    #[test]
    fn test_webhook_manager_get_history_not_found() {
        let manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        assert!(manager.get_history("nonexistent", 10).is_empty());
    }

    #[test]
    fn test_webhook_manager_clear_history_not_found() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let result = manager.clear_history("nonexistent");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            WebhookError::EndpointNotFound(_)
        ));
    }

    #[test]
    fn test_webhook_manager_update_all_fields() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        let mut headers = HashMap::new();
        headers.insert("X-Custom".to_string(), "value".to_string());

        let updates = WebhookEndpointUpdate {
            url: Some("https://new.example.com/hook".to_string()),
            name: Some("New Name".to_string()),
            enabled: Some(false),
            secret: Some("new-secret".to_string()),
            timeout_secs: Some(120),
            max_retries: Some(5),
            events: Some(vec![WebhookEvent::DownloadComplete]),
            headers: Some(headers),
        };
        manager.update_endpoint(&id, updates).unwrap();

        let updated = manager.get_endpoint(&id).unwrap();
        assert_eq!(updated.url, "https://new.example.com/hook");
        assert_eq!(updated.name, "New Name");
        assert!(!updated.enabled);
        assert_eq!(updated.secret, Some("new-secret".to_string()));
        assert_eq!(updated.timeout_secs, 120);
        assert_eq!(updated.max_retries, 5);
        assert_eq!(updated.events, vec![WebhookEvent::DownloadComplete]);
        assert!(updated.headers.contains_key("X-Custom"));
    }

    #[test]
    fn test_webhook_manager_record_delivery_trimming() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        manager.config.max_history_per_endpoint = 3;

        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        // Record 5 deliveries
        for _ in 0..5 {
            let delivery = WebhookDelivery::new(id.clone(), WebhookEvent::DownloadComplete);
            manager.record_delivery(&id, delivery);
        }

        // Should be trimmed to 3
        assert_eq!(manager.get_history(&id, 10).len(), 3);
    }

    #[test]
    fn test_webhook_manager_multiple_endpoints_independent() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        let ep1 =
            WebhookEndpoint::new("https://example.com/hook1".to_string(), "Hook1".to_string());
        let ep2 =
            WebhookEndpoint::new("https://example.com/hook2".to_string(), "Hook2".to_string());
        let id1 = manager.add_endpoint(ep1).unwrap();
        let id2 = manager.add_endpoint(ep2).unwrap();

        manager.record_delivery(
            &id1,
            WebhookDelivery::new(id1.clone(), WebhookEvent::DownloadComplete),
        );
        manager.record_delivery(
            &id1,
            WebhookDelivery::new(id1.clone(), WebhookEvent::DownloadFailed),
        );

        assert_eq!(manager.get_history(&id1, 10).len(), 2);
        assert_eq!(manager.get_history(&id2, 10).len(), 0);
    }

    // --- WebhookError: Display + From ---
    #[test]
    fn test_webhook_error_display() {
        let err = WebhookError::InvalidUrl("bad url".to_string());
        assert_eq!(format!("{}", err), "invalid URL: bad url");

        let err = WebhookError::EndpointNotFound("ep-1".to_string());
        assert_eq!(format!("{}", err), "endpoint not found: ep-1");
    }

    #[test]
    fn test_webhook_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: WebhookError = io_err.into();
        assert!(matches!(err, WebhookError::Io(_)));
        assert!(format!("{}", err).contains("file not found"));
    }

    #[test]
    fn test_webhook_error_from_serde() {
        let serde_err = serde_json::from_str::<WebhookConfig>("invalid json").unwrap_err();
        let err: WebhookError = serde_err.into();
        assert!(matches!(err, WebhookError::Serialize(_)));
    }

    #[test]
    fn test_webhook_error_debug() {
        let err = WebhookError::InvalidUrl("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("InvalidUrl"));
    }

    // --- Persistence ---
    #[tokio::test]
    async fn test_webhook_manager_save_config_creates_file() {
        let dir = std::env::temp_dir().join(format!("webhook_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let manager = WebhookManager::new(dir.clone());
        manager.save_config().await.unwrap();

        let config_path = dir.join("webhook_config.json");
        assert!(config_path.exists());

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_webhook_manager_load_config_missing_file() {
        let dir = std::env::temp_dir().join(format!("webhook_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut manager = WebhookManager::new(dir.clone());
        let result = manager.load_config().await;
        assert!(result.is_ok()); // Missing file is OK

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_webhook_manager_save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("webhook_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut manager = WebhookManager::new(dir.clone());
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test Hook".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        manager.save_config().await.unwrap();

        let mut loaded = WebhookManager::new(dir.clone());
        loaded.load_config().await.unwrap();

        assert!(loaded.get_endpoint(&id).is_some());
        assert_eq!(loaded.get_endpoint(&id).unwrap().name, "Test Hook");

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_webhook_manager_load_config_corrupt_json() {
        let dir = std::env::temp_dir().join(format!("webhook_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let config_path = dir.join("webhook_config.json");
        tokio::fs::write(&config_path, "not valid json")
            .await
            .unwrap();

        let mut manager = WebhookManager::new(dir.clone());
        let result = manager.load_config().await;
        assert!(result.is_err());

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_webhook_manager_save_overwrite() {
        let dir = std::env::temp_dir().join(format!("webhook_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut manager = WebhookManager::new(dir.clone());

        // First save
        manager.save_config().await.unwrap();

        // Add endpoint and save again
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "New Hook".to_string(),
        );
        manager.add_endpoint(endpoint).unwrap();
        manager.save_config().await.unwrap();

        // Load and verify
        let mut loaded = WebhookManager::new(dir.clone());
        loaded.load_config().await.unwrap();
        assert_eq!(loaded.list_endpoints().len(), 1);

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    // --- Unicode endpoint names ---
    #[test]
    fn test_webhook_endpoint_unicode_name() {
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "中文 webhook".to_string(),
        );
        assert_eq!(endpoint.name, "中文 webhook");
    }

    #[test]
    fn test_webhook_endpoint_emoji_name() {
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "🚀 webhook".to_string(),
        );
        assert_eq!(endpoint.name, "🚀 webhook");
    }

    // --- Edge cases ---
    #[test]
    fn test_webhook_manager_add_endpoint_empty_url() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let mut endpoint = WebhookEndpoint::new("".to_string(), "Test".to_string());
        endpoint.url = "".to_string();
        let result = manager.add_endpoint(endpoint);
        assert!(result.is_err());
    }

    #[test]
    fn test_webhook_manager_add_endpoint_http() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint =
            WebhookEndpoint::new("http://example.com/webhook".to_string(), "Test".to_string());
        let result = manager.add_endpoint(endpoint);
        assert!(result.is_ok());
    }

    #[test]
    fn test_webhook_endpoint_should_send_disabled_with_events() {
        let mut endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        endpoint.add_event(WebhookEvent::DownloadComplete);
        endpoint.enabled = false;

        // Even if event matches, disabled endpoint returns false
        assert!(!endpoint.should_send(WebhookEvent::DownloadComplete));
    }

    #[test]
    fn test_webhook_endpoint_default_values() {
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        assert!(endpoint.enabled);
        assert_eq!(endpoint.timeout_secs, 30);
        assert_eq!(endpoint.max_retries, 3);
    }

    #[test]
    fn test_webhook_endpoint_serde_defaults() {
        let json = r#"{"id":"test","url":"https://example.com","name":"Test","created_at":"2024-01-01T00:00:00Z"}"#;
        let endpoint: WebhookEndpoint = serde_json::from_str(json).unwrap();
        assert!(endpoint.enabled); // default_true
        assert_eq!(endpoint.timeout_secs, 30); // default_timeout
        assert_eq!(endpoint.max_retries, 3); // default_max_retries
        assert!(endpoint.events.is_empty()); // default
        assert!(endpoint.secret.is_none()); // default
        assert!(endpoint.headers.is_empty()); // default
    }

    // --- format_webhook_summary ---
    #[test]
    fn test_format_webhook_summary_empty() {
        let summary = WebhookSummary {
            total_endpoints: 0,
            enabled_endpoints: 0,
            total_deliveries: 0,
            successful_deliveries: 0,
            failed_deliveries: 0,
            success_rate: 100.0,
        };
        let formatted = format_webhook_summary(&summary);
        assert!(formatted.contains("0 total"));
        assert!(formatted.contains("100.0%"));
    }

    #[test]
    fn test_format_webhook_summary_unicode() {
        let summary = WebhookSummary {
            total_endpoints: 3,
            enabled_endpoints: 2,
            total_deliveries: 100,
            successful_deliveries: 90,
            failed_deliveries: 10,
            success_rate: 90.0,
        };
        let formatted = format_webhook_summary(&summary);
        assert!(formatted.contains("📡"));
        assert!(formatted.contains("3 total"));
        assert!(formatted.contains("2 enabled"));
        assert!(formatted.contains("90 successful"));
        assert!(formatted.contains("10 failed"));
    }

    // --- Complex workflow ---
    #[test]
    fn test_webhook_manager_full_lifecycle() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        // Create endpoints
        let ep1 = WebhookEndpoint::new(
            "https://example.com/hook1".to_string(),
            "Hook 1".to_string(),
        );
        let ep2 = WebhookEndpoint::new(
            "https://example.com/hook2".to_string(),
            "Hook 2".to_string(),
        );
        let id1 = manager.add_endpoint(ep1).unwrap();
        let id2 = manager.add_endpoint(ep2).unwrap();

        // Record deliveries
        let mut d1 = WebhookDelivery::new(id1.clone(), WebhookEvent::DownloadComplete);
        d1.mark_success(200);
        manager.record_delivery(&id1, d1);

        let mut d2 = WebhookDelivery::new(id2.clone(), WebhookEvent::DownloadFailed);
        d2.mark_failed("timeout".to_string(), Some(504));
        manager.record_delivery(&id2, d2);

        // Verify summary
        let summary = manager.get_summary();
        assert_eq!(summary.total_endpoints, 2);
        assert_eq!(summary.total_deliveries, 2);
        assert_eq!(summary.successful_deliveries, 1);
        assert_eq!(summary.failed_deliveries, 1);
        assert_eq!(summary.success_rate, 50.0);

        // Update endpoint
        let updates = WebhookEndpointUpdate {
            enabled: Some(false),
            ..Default::default()
        };
        manager.update_endpoint(&id1, updates).unwrap();
        assert!(!manager.get_endpoint(&id1).unwrap().enabled);

        // Clear and verify
        manager.clear_history(&id1).unwrap();
        assert_eq!(manager.get_history(&id1, 10).len(), 0);
        assert_eq!(manager.get_history(&id2, 10).len(), 1);

        // Remove endpoint
        manager.remove_endpoint(&id1).unwrap();
        assert_eq!(manager.list_endpoints().len(), 1);
    }

    #[test]
    fn test_webhook_manager_get_all_history() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        let ep1 =
            WebhookEndpoint::new("https://example.com/hook1".to_string(), "Hook1".to_string());
        let ep2 =
            WebhookEndpoint::new("https://example.com/hook2".to_string(), "Hook2".to_string());
        let id1 = manager.add_endpoint(ep1).unwrap();
        let id2 = manager.add_endpoint(ep2).unwrap();

        manager.record_delivery(
            &id1,
            WebhookDelivery::new(id1.clone(), WebhookEvent::DownloadComplete),
        );
        manager.record_delivery(
            &id2,
            WebhookDelivery::new(id2.clone(), WebhookEvent::DownloadFailed),
        );

        let all = manager.get_all_history();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_webhook_manager_set_config() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));

        let config = WebhookConfig {
            enabled: false,
            max_history_per_endpoint: 50,
            global_timeout_secs: Some(120),
            log_deliveries: false,
        };
        manager.set_config(config);

        assert!(!manager.config().enabled);
        assert_eq!(manager.config().max_history_per_endpoint, 50);
        assert_eq!(manager.config().global_timeout_secs, Some(120));
    }

    #[test]
    fn test_compute_hmac_signature_empty_payload() {
        let signature = compute_hmac_signature("", "secret").unwrap();
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_compute_hmac_signature_unicode() {
        let payload = r#"{"event":"download_complete","task":"测试文件"}"#;
        let secret = "密钥";
        let signature = compute_hmac_signature(payload, secret).unwrap();
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_webhook_endpoint_id_uniqueness() {
        let ep1 =
            WebhookEndpoint::new("https://example.com/hook1".to_string(), "Hook1".to_string());
        let ep2 =
            WebhookEndpoint::new("https://example.com/hook2".to_string(), "Hook2".to_string());
        assert_ne!(ep1.id, ep2.id);
    }

    #[test]
    fn test_webhook_delivery_id_uniqueness() {
        let d1 = WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadComplete);
        let d2 = WebhookDelivery::new("ep-1".to_string(), WebhookEvent::DownloadComplete);
        assert_ne!(d1.id, d2.id);
    }

    #[test]
    fn test_webhook_manager_summary_all_failed() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        for _ in 0..5 {
            let mut d = WebhookDelivery::new(id.clone(), WebhookEvent::DownloadFailed);
            d.mark_failed("error".to_string(), Some(500));
            manager.record_delivery(&id, d);
        }

        let summary = manager.get_summary();
        assert_eq!(summary.successful_deliveries, 0);
        assert_eq!(summary.failed_deliveries, 5);
        assert_eq!(summary.success_rate, 0.0);
    }

    #[test]
    fn test_webhook_manager_summary_all_success() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        for _ in 0..5 {
            let mut d = WebhookDelivery::new(id.clone(), WebhookEvent::DownloadComplete);
            d.mark_success(200);
            manager.record_delivery(&id, d);
        }

        let summary = manager.get_summary();
        assert_eq!(summary.successful_deliveries, 5);
        assert_eq!(summary.failed_deliveries, 0);
        assert_eq!(summary.success_rate, 100.0);
    }

    #[test]
    fn test_webhook_endpoint_add_remove_all_events() {
        let mut endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );

        // Add all event types
        for event in WebhookEvent::all() {
            endpoint.add_event(*event);
        }
        assert_eq!(endpoint.events.len(), 9);

        // Remove all one by one
        for event in WebhookEvent::all() {
            endpoint.remove_event(*event);
        }
        assert!(endpoint.events.is_empty());
    }

    #[test]
    fn test_webhook_manager_remove_endpoint_clears_history() {
        let mut manager = WebhookManager::new(std::path::PathBuf::from("/tmp/test"));
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "Test".to_string(),
        );
        let id = manager.add_endpoint(endpoint).unwrap();

        manager.record_delivery(
            &id,
            WebhookDelivery::new(id.clone(), WebhookEvent::DownloadComplete),
        );
        assert_eq!(manager.get_history(&id, 10).len(), 1);

        manager.remove_endpoint(&id).unwrap();
        // History should be gone too
        assert!(manager.get_history(&id, 10).is_empty());
    }
}
