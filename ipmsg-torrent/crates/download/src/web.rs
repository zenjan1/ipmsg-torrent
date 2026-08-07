//! Web UI for download management
//!
//! Provides a REST API, WebSocket real-time updates, and HTML frontend.

use crate::{DownloadManager, DownloadState, DownloadTask};
use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Web server state
pub struct WebState {
    pub manager: Arc<DownloadManager>,
    pub server_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl WebState {
    pub fn new(manager: Arc<DownloadManager>) -> Self {
        Self {
            manager,
            server_handle: Mutex::new(None),
        }
    }
}

/// Task info for API responses
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub size: u64,
    pub downloaded: u64,
    pub progress: f32,
    pub speed_bps: f64,
    pub state: String,
    pub error: Option<String>,
    pub tags: Vec<String>,
    /// Task IDs this task depends on (for dependency visualization)
    pub depends_on: Vec<String>,
}

impl From<DownloadTask> for TaskInfo {
    fn from(task: DownloadTask) -> Self {
        let progress = task.progress();
        let state = task.state_label().to_string();
        Self {
            id: task.id,
            name: task.name,
            protocol: format!("{:?}", task.protocol),
            size: task.size,
            downloaded: task.downloaded,
            progress,
            speed_bps: task.speed_bps,
            state,
            error: task.error,
            tags: task.tags,
            depends_on: task.depends_on,
        }
    }
}

/// Request to add a download
#[derive(Debug, Deserialize)]
pub struct AddDownloadRequest {
    pub url: String,
}

/// Request to batch import multiple URLs
#[derive(Debug, Deserialize)]
pub struct BatchImportRequest {
    pub urls: Vec<String>,
}

/// Response for batch import
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchImportResponse {
    pub success: bool,
    pub total: usize,
    pub added: usize,
    pub skipped: usize,
    pub failed: usize,
    pub message: String,
}

/// Response for task operations
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResponse {
    pub success: bool,
    pub task_id: Option<String>,
    pub message: String,
}

/// Create the web router
pub fn create_router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/api/tasks", get(list_tasks))
        .route("/api/tasks/:id", get(get_task))
        .route("/api/tasks/:id/pause", post(pause_task))
        .route("/api/tasks/:id/resume", post(resume_task))
        .route("/api/tasks/:id/remove", post(remove_task))
        .route("/api/tasks/:id/tags", post(add_tags))
        .route("/api/tasks/:id/tags/remove", post(remove_tags))
        .route("/api/tags", get(list_all_tags))
        .route("/api/download", post(add_download))
        .route("/api/status", get(get_status))
        .route("/api/stats", get(get_stats))
        .route("/api/batch/pause-all", post(pause_all))
        .route("/api/batch/resume-all", post(resume_all))
        .route("/api/batch/remove-completed", post(remove_completed))
        .route("/api/batch/remove-failed", post(remove_failed))
        .route("/api/batch-import", post(batch_import))
        .route("/api/bandwidth", get(get_bandwidth))
        .route("/api/bandwidth/history", get(get_bandwidth_history))
        .route("/api/bandwidth/summary", get(get_bandwidth_summary))
        .route("/api/deps", get(get_deps))
        .route("/api/proxy", get(get_proxy))
        .route("/api/proxy", post(set_proxy))
        .route("/api/proxy/disable", post(disable_proxy))
        .route("/api/proxy/test", post(test_proxy))
        .route("/api/notifications/history", get(get_notification_history))
        .route("/api/ws", get(ws_handler))
        .route("/", get(index_html))
        .with_state(state)
}

/// List all download tasks
async fn list_tasks(State(state): State<Arc<WebState>>) -> Json<Vec<TaskInfo>> {
    let tasks = state.manager.list_tasks().await;
    Json(tasks.into_iter().map(TaskInfo::from).collect())
}

/// Get a specific task by ID
async fn get_task(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<TaskInfo>, StatusCode> {
    state
        .manager
        .get_task(&id)
        .await
        .map(|t| Json(TaskInfo::from(t)))
        .ok_or(StatusCode::NOT_FOUND)
}

/// Pause a task
async fn pause_task(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let success = state.manager.pause_task(&id).await;
    Json(TaskResponse {
        success,
        task_id: Some(id),
        message: if success {
            "Task paused".to_string()
        } else {
            "Failed to pause task".to_string()
        },
    })
}

/// Resume a task
async fn resume_task(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let success = state.manager.resume_task(&id).await;
    Json(TaskResponse {
        success,
        task_id: Some(id),
        message: if success {
            "Task resumed".to_string()
        } else {
            "Failed to resume task".to_string()
        },
    })
}

/// Remove a task
async fn remove_task(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let success = state.manager.remove_task(&id).await;
    Json(TaskResponse {
        success,
        task_id: Some(id),
        message: if success {
            "Task removed".to_string()
        } else {
            "Failed to remove task".to_string()
        },
    })
}

/// Add a new download
async fn add_download(
    State(state): State<Arc<WebState>>,
    Json(req): Json<AddDownloadRequest>,
) -> Result<Json<TaskResponse>, StatusCode> {
    let result = if req.url.starts_with("magnet:") {
        state.manager.add_magnet(&req.url).await
    } else if req.url.starts_with("ed2k://") {
        // Parse ed2k link (simplified)
        return Err(StatusCode::NOT_IMPLEMENTED);
    } else if req.url.ends_with(".torrent") {
        // Download torrent file first, then add
        return Err(StatusCode::NOT_IMPLEMENTED);
    } else {
        state.manager.add_url(&req.url).await
    };

    match result {
        Ok(task_id) => Ok(Json(TaskResponse {
            success: true,
            task_id: Some(task_id),
            message: "Download added".to_string(),
        })),
        Err(e) => Ok(Json(TaskResponse {
            success: false,
            task_id: None,
            message: format!("Failed to add download: {}", e),
        })),
    }
}

/// Batch import multiple URLs
async fn batch_import(
    State(state): State<Arc<WebState>>,
    Json(req): Json<BatchImportRequest>,
) -> Json<BatchImportResponse> {
    let results = state.manager.import_urls(&req.urls).await;

    let mut added = 0;
    let mut skipped = 0;
    let mut failed = 0;

    for result in &results {
        match result.outcome {
            crate::ImportOutcome::Added(_) => added += 1,
            crate::ImportOutcome::SkippedDuplicate => skipped += 1,
            crate::ImportOutcome::Failed(_) => failed += 1,
        }
    }

    let total = results.len();
    let success = failed == 0;
    let message = if success {
        format!("Successfully imported {} URLs", added)
    } else {
        format!("Imported {} URLs, {} failed", added, failed)
    };

    Json(BatchImportResponse {
        success,
        total,
        added,
        skipped,
        failed,
        message,
    })
}

/// Get server status
async fn get_status(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let tasks = state.manager.list_tasks().await;
    let running = tasks
        .iter()
        .filter(|t| t.state == DownloadState::Downloading)
        .count();
    let total_speed: f64 = tasks.iter().map(|t| t.speed_bps).sum();

    Json(serde_json::json!({
        "total_tasks": tasks.len(),
        "running_tasks": running,
        "total_speed_bps": total_speed,
    }))
}

/// Get detailed download statistics
async fn get_stats(State(state): State<Arc<WebState>>) -> Json<crate::DownloadStats> {
    let stats = state.manager.get_stats().await;
    Json(stats)
}

/// Get bandwidth monitoring dashboard
async fn get_bandwidth(State(state): State<Arc<WebState>>) -> Json<crate::BandwidthDashboard> {
    let tasks = state.manager.list_tasks().await;
    let task_speeds: Vec<_> = tasks
        .iter()
        .filter(|t| t.state == DownloadState::Downloading)
        .map(|t| (t.id.clone(), t.name.clone(), t.speed_bps, t.downloaded))
        .collect();

    let dashboard = state
        .manager
        .bandwidth_monitor()
        .dashboard(task_speeds)
        .await;
    Json(dashboard)
}

/// Query parameters for bandwidth history
#[derive(Debug, Deserialize)]
pub struct BandwidthHistoryQuery {
    /// Time window in seconds (default: 3600 = 1 hour)
    pub window: Option<u64>,
    /// Maximum number of samples to return (default: all in window)
    pub limit: Option<usize>,
}

/// Response for bandwidth history
#[derive(Debug, Serialize, Deserialize)]
pub struct BandwidthHistoryResponse {
    pub samples: Vec<crate::BandwidthSample>,
    pub window_secs: u64,
    pub sample_count: usize,
}

/// Get bandwidth trend summary
async fn get_bandwidth_summary(
    State(state): State<Arc<WebState>>,
) -> Json<crate::BandwidthTrendSummary> {
    let summary = state
        .manager
        .bandwidth_monitor()
        .compute_trend_summary()
        .await;
    Json(summary)
}

/// Get bandwidth history samples for charting
async fn get_bandwidth_history(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<BandwidthHistoryQuery>,
) -> Json<BandwidthHistoryResponse> {
    let window_secs = params.window.unwrap_or(3600);
    let limit = params.limit.unwrap_or(usize::MAX);

    let history = state.manager.bandwidth_monitor().history().await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let cutoff = now.saturating_sub(window_secs);

    let samples: Vec<crate::BandwidthSample> = history
        .into_iter()
        .filter(|s| s.timestamp >= cutoff)
        .take(limit)
        .collect();

    let sample_count = samples.len();
    Json(BandwidthHistoryResponse {
        samples,
        window_secs,
        sample_count,
    })
}

/// Dependency graph node for API response
#[derive(Debug, Serialize, Deserialize)]
pub struct DepNode {
    pub id: String,
    pub name: String,
    pub state: String,
    pub depends_on: Vec<String>,
}

/// Dependency graph response
#[derive(Debug, Serialize, Deserialize)]
pub struct DepGraphResponse {
    pub nodes: Vec<DepNode>,
    pub edges: Vec<DepEdge>,
}

/// Dependency edge (from depends on to)
#[derive(Debug, Serialize, Deserialize)]
pub struct DepEdge {
    pub from: String,
    pub to: String,
}

/// Get task dependency graph
async fn get_deps(State(state): State<Arc<WebState>>) -> Json<DepGraphResponse> {
    let tasks = state.manager.list_tasks().await;
    let task_ids: std::collections::HashSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();

    let nodes: Vec<DepNode> = tasks
        .iter()
        .map(|t| DepNode {
            id: t.id.clone(),
            name: t.name.clone(),
            state: t.state_label().to_string(),
            depends_on: t.depends_on.clone(),
        })
        .collect();

    let edges: Vec<DepEdge> = tasks
        .iter()
        .flat_map(|t| {
            t.depends_on
                .iter()
                .filter(|dep_id| task_ids.contains(dep_id.as_str()))
                .map(|dep_id| DepEdge {
                    from: dep_id.clone(),
                    to: t.id.clone(),
                })
        })
        .collect();

    Json(DepGraphResponse { nodes, edges })
}

/// Pause all running downloads
async fn pause_all(State(state): State<Arc<WebState>>) -> Json<TaskResponse> {
    let count = state.manager.pause_all().await;
    Json(TaskResponse {
        success: true,
        task_id: None,
        message: format!("Paused {} tasks", count),
    })
}

/// Resume all paused downloads
async fn resume_all(State(state): State<Arc<WebState>>) -> Json<TaskResponse> {
    let count = state.manager.resume_all().await;
    Json(TaskResponse {
        success: true,
        task_id: None,
        message: format!("Resumed {} tasks", count),
    })
}

/// Remove all completed downloads
async fn remove_completed(State(state): State<Arc<WebState>>) -> Json<TaskResponse> {
    let count = state.manager.remove_completed().await;
    Json(TaskResponse {
        success: true,
        task_id: None,
        message: format!("Removed {} completed tasks", count),
    })
}

/// Remove all failed downloads
async fn remove_failed(State(state): State<Arc<WebState>>) -> Json<TaskResponse> {
    let count = state.manager.remove_failed().await;
    Json(TaskResponse {
        success: true,
        task_id: None,
        message: format!("Removed {} failed tasks", count),
    })
}

/// Request to set proxy configuration
#[derive(Debug, Deserialize)]
pub struct SetProxyRequest {
    /// Proxy URL (e.g., "socks5://127.0.0.1:1080" or "http://user:pass@proxy:8080")
    /// Set to empty string or null to disable proxy
    pub url: String,
}

/// Response for proxy status
#[derive(Debug, Serialize, Deserialize)]
pub struct ProxyStatusResponse {
    pub enabled: bool,
    pub proxy_type: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub has_auth: bool,
    pub url: Option<String>,
}

impl From<Option<crate::proxy::ProxyConfig>> for ProxyStatusResponse {
    fn from(config: Option<crate::proxy::ProxyConfig>) -> Self {
        match config {
            Some(cfg) => Self {
                enabled: true,
                proxy_type: Some(cfg.proxy_type.label().to_string()),
                host: Some(cfg.host.clone()),
                port: Some(cfg.port),
                has_auth: cfg.auth.is_some(),
                url: Some(cfg.to_url()),
            },
            None => Self {
                enabled: false,
                proxy_type: None,
                host: None,
                port: None,
                has_auth: false,
                url: None,
            },
        }
    }
}

/// Request to update tags
#[derive(Debug, Deserialize)]
pub struct UpdateTagsRequest {
    pub tags: Vec<String>,
}

/// Add tags to a task
async fn add_tags(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<UpdateTagsRequest>,
) -> Json<TaskResponse> {
    let success = state.manager.add_tags(&id, req.tags).await;
    Json(TaskResponse {
        success,
        task_id: Some(id),
        message: if success {
            "Tags added".to_string()
        } else {
            "Task not found".to_string()
        },
    })
}

/// Remove tags from a task
async fn remove_tags(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<UpdateTagsRequest>,
) -> Json<TaskResponse> {
    let success = state.manager.remove_tags(&id, req.tags).await;
    Json(TaskResponse {
        success,
        task_id: Some(id),
        message: if success {
            "Tags removed".to_string()
        } else {
            "Task not found".to_string()
        },
    })
}

/// List all unique tags
async fn list_all_tags(State(state): State<Arc<WebState>>) -> Json<Vec<String>> {
    Json(state.manager.list_all_tags().await)
}

/// Get current proxy configuration
async fn get_proxy(State(state): State<Arc<WebState>>) -> Json<ProxyStatusResponse> {
    let config = state.manager.get_proxy().await;
    Json(ProxyStatusResponse::from(config))
}

/// Set proxy configuration
async fn set_proxy(
    State(state): State<Arc<WebState>>,
    Json(req): Json<SetProxyRequest>,
) -> Json<TaskResponse> {
    if req.url.is_empty() {
        // Disable proxy
        state.manager.set_proxy(None).await;
        Json(TaskResponse {
            success: true,
            task_id: None,
            message: "Proxy disabled".to_string(),
        })
    } else {
        // Parse and set proxy
        match crate::proxy::ProxyConfig::parse(&req.url) {
            Ok(config) => {
                state.manager.set_proxy(Some(config)).await;
                Json(TaskResponse {
                    success: true,
                    task_id: None,
                    message: "Proxy configured".to_string(),
                })
            }
            Err(e) => Json(TaskResponse {
                success: false,
                task_id: None,
                message: format!("Invalid proxy URL: {}", e),
            }),
        }
    }
}

/// Disable proxy configuration
async fn disable_proxy(State(state): State<Arc<WebState>>) -> Json<TaskResponse> {
    state.manager.set_proxy(None).await;
    Json(TaskResponse {
        success: true,
        task_id: None,
        message: "Proxy disabled".to_string(),
    })
}

/// Test proxy connection
async fn test_proxy(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let result = state.manager.test_proxy_connection().await;
    match result {
        Some(test_result) => {
            let mut value = serde_json::to_value(&test_result).unwrap_or_default();
            value["display"] = serde_json::Value::String(test_result.format_display());
            Json(value)
        }
        None => Json(serde_json::json!({
            "success": false,
            "error": "No proxy configured",
            "display": "No proxy configured"
        })),
    }
}

/// Get notification history
async fn get_notification_history(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let history = state.manager.notification_history();
    Json(serde_json::json!({
        "entries": history.get_all(),
        "count": history.len()
    }))
}

/// WebSocket upgrade handler
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<WebState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

/// Handle a WebSocket connection.
///
/// Spawns two tasks:
/// 1. Forward broadcast events from DownloadManager to the WebSocket client
/// 2. Receive client messages (ping/pong or commands) and keep connection alive
async fn handle_ws(socket: WebSocket, state: Arc<WebState>) {
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to task events
    let mut event_rx = state.manager.subscribe();

    // Send initial snapshot of all tasks
    let tasks = state.manager.list_tasks().await;
    let snapshot: Vec<TaskInfo> = tasks.into_iter().map(TaskInfo::from).collect();
    if let Ok(json) = serde_json::to_string(&snapshot) {
        let _ = sender.send(Message::Text(json)).await;
    }

    // Forward broadcast events to WebSocket
    let send_task = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            if let Ok(json) = serde_json::to_string(&event)
                && sender.send(Message::Text(json)).await.is_err()
            {
                break;
            }
        }
    });

    // Drain client messages (we don't expect any, but need to keep connection alive)
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(_)) = receiver.next().await {
            // Client messages are ignored for now
        }
    });

    // Wait for either task to finish (client disconnect or broadcast closed)
    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }
}

/// Serve the index HTML page
async fn index_html() -> &'static str {
    include_str!("web/index.html")
}

/// Start the web server
pub async fn start_server(
    state: Arc<WebState>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = create_router(state.clone());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Web UI started at http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaskEvent, TaskInfoEvent};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> Arc<WebState> {
        let manager = Arc::new(DownloadManager::new(std::path::PathBuf::from("/tmp/test")));
        Arc::new(WebState::new(manager))
    }

    #[tokio::test]
    async fn test_list_tasks_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tasks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_status() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_index_html() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_task() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tasks/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_task_event_serialization() {
        let event = TaskEvent::Added {
            task: TaskInfoEvent {
                id: "abc".into(),
                name: "test.txt".into(),
                protocol: "Torrent".into(),
                size: 1024,
                downloaded: 512,
                progress: 50.0,
                speed_bps: 1000.0,
                state: "downloading".into(),
                error: None,
                tags: Vec::new(),
                priority: "normal".into(),
                bandwidth_weight: 1,
                depends_on: Vec::new(),
                notes: None,
                queue_position: None,
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"task_added\""));
        assert!(json.contains("\"id\":\"abc\""));
    }

    #[tokio::test]
    async fn test_subscribe_receives_events() {
        let manager = Arc::new(DownloadManager::new(std::path::PathBuf::from(
            "/tmp/test_ws",
        )));
        let mut rx = manager.subscribe();

        // Adding a URL will fail (no real server), but we just want to verify
        // the event system doesn't panic. We test subscribe() returns a valid receiver.
        // Direct event emission test:
        manager.emit_event_for_test(TaskEvent::Removed {
            task_id: "test-id".into(),
        });

        let event = rx.try_recv().unwrap();
        match event {
            TaskEvent::Removed { task_id } => assert_eq!(task_id, "test-id"),
            _ => panic!("Expected Removed event"),
        }
    }

    #[tokio::test]
    async fn test_get_stats() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_pause_all() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/batch/pause-all")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_resume_all() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/batch/resume-all")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_remove_completed() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/batch/remove-completed")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_remove_failed() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/batch/remove-failed")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_import_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/batch-import")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"urls":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: BatchImportResponse = serde_json::from_slice(&body).unwrap();
        assert!(resp.success);
        assert_eq!(resp.total, 0);
        assert_eq!(resp.added, 0);
        assert_eq!(resp.failed, 0);
    }

    #[tokio::test]
    async fn test_batch_import_unsupported_urls() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/batch-import")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"urls":["ftp://invalid","mailto://bad"]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: BatchImportResponse = serde_json::from_slice(&body).unwrap();
        // Both should fail since we can't actually connect
        assert_eq!(resp.total, 2);
    }

    #[tokio::test]
    async fn test_get_proxy_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/proxy")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ProxyStatusResponse = serde_json::from_slice(&body).unwrap();
        assert!(!resp.enabled);
        assert!(resp.url.is_none());
    }

    #[tokio::test]
    async fn test_set_proxy_valid() {
        let state = test_state();
        let app = create_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/proxy")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"url":"socks5://127.0.0.1:1080"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: TaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(resp.success);
        assert_eq!(resp.message, "Proxy configured");

        // Verify proxy was set
        let proxy = state.manager.get_proxy().await;
        assert!(proxy.is_some());
        let cfg = proxy.unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 1080);
    }

    #[tokio::test]
    async fn test_set_proxy_invalid() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/proxy")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"url":"invalid-url"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: TaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(!resp.success);
        assert!(resp.message.contains("Invalid proxy URL"));
    }

    #[tokio::test]
    async fn test_disable_proxy() {
        let state = test_state();

        // Set proxy first
        state
            .manager
            .set_proxy(Some(crate::proxy::ProxyConfig::new(
                crate::proxy::ProxyType::Socks5,
                "127.0.0.1".into(),
                1080,
            )))
            .await;

        let app = create_router(state.clone());

        // Disable it
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/proxy/disable")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: TaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(resp.success);
        assert_eq!(resp.message, "Proxy disabled");

        // Verify proxy was disabled
        let proxy = state.manager.get_proxy().await;
        assert!(proxy.is_none());
    }

    #[tokio::test]
    async fn test_set_proxy_empty_url_disables() {
        let state = test_state();

        // Set proxy first
        state
            .manager
            .set_proxy(Some(crate::proxy::ProxyConfig::new(
                crate::proxy::ProxyType::Http,
                "proxy.example.com".into(),
                8080,
            )))
            .await;

        let app = create_router(state.clone());

        // Set empty URL should disable
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/proxy")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"url":""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: TaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(resp.success);
        assert_eq!(resp.message, "Proxy disabled");

        let proxy = state.manager.get_proxy().await;
        assert!(proxy.is_none());
    }

    #[tokio::test]
    async fn test_proxy_status_response_format() {
        let config = crate::proxy::ProxyConfig::with_auth(
            crate::proxy::ProxyType::Socks5,
            "127.0.0.1".into(),
            1080,
            "user".into(),
            "pass".into(),
        );
        let resp = ProxyStatusResponse::from(Some(config));
        assert!(resp.enabled);
        assert_eq!(resp.proxy_type, Some("socks5".into()));
        assert_eq!(resp.host, Some("127.0.0.1".into()));
        assert_eq!(resp.port, Some(1080));
        assert!(resp.has_auth);
        assert!(resp.url.is_some());
    }

    #[tokio::test]
    async fn test_proxy_test_no_proxy() {
        let state = test_state();
        let app = create_router(state);

        // Test proxy when none is configured
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/proxy/test")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp["success"], false);
        assert_eq!(resp["error"], "No proxy configured");
    }

    #[tokio::test]
    async fn test_get_deps_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/deps")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: DepGraphResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.nodes.len(), 0);
        assert_eq!(resp.edges.len(), 0);
    }

    #[tokio::test]
    async fn test_get_bandwidth_history() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/bandwidth/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: BandwidthHistoryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.window_secs, 3600);
        assert_eq!(resp.sample_count, 0);
    }

    #[tokio::test]
    async fn test_get_bandwidth_history_with_window() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/bandwidth/history?window=300&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: BandwidthHistoryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.window_secs, 300);
    }

    #[tokio::test]
    async fn test_task_info_includes_depends_on() {
        let task = DownloadTask {
            id: "task-1".into(),
            name: "test.txt".into(),
            protocol: crate::DownloadProtocol::Xunlei,
            save_path: std::path::PathBuf::from("/tmp"),
            size: 1024,
            downloaded: 0,
            speed_bps: 0.0,
            state: crate::DownloadState::Queued,
            error: None,
            tags: vec![],
            priority: crate::DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: vec!["task-0".into()],
            notes: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let info = TaskInfo::from(task);
        assert_eq!(info.depends_on, vec!["task-0".to_string()]);
    }
}
