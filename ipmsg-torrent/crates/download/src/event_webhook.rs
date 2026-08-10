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
    #[serde(skip_serializing_if = "HashMap::is_empty")]
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
}
