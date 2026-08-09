//! Web UI for download management
//!
//! Provides a REST API, WebSocket real-time updates, and HTML frontend.

use crate::{DownloadManager, DownloadState, DownloadTask};
use axum::{
    Json, Router,
    extract::{
        Path, State,
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
    /// Sequential download mode for torrents
    pub sequential_mode: bool,
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
            sequential_mode: task.sequential_mode,
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
        .route("/api/task-speed", get(get_task_speed))
        .route("/api/task-speed", post(set_task_speed))
        .route("/api/checksum", post(set_checksum))
        .route("/api/hooks", get(list_hooks))
        .route("/api/hooks", post(add_hook))
        .route("/api/hooks/:id", post(update_hook))
        .route("/api/hooks/:id/remove", post(remove_hook))
        .route("/api/feeds", get(list_feeds))
        .route("/api/feeds", post(add_feed))
        .route("/api/feeds/:id/remove", post(remove_feed))
        .route("/api/feeds/:id/enable", post(enable_feed))
        .route("/api/feeds/:id/disable", post(disable_feed))
        .route("/api/feeds/:id/poll", post(poll_feed))
        .route("/api/eta", get(get_all_eta))
        .route("/api/eta/:id", get(get_task_eta))
        .route("/api/auto-rules", get(list_auto_rules))
        .route("/api/auto-rules", post(add_auto_rule))
        .route("/api/auto-rules/:id/remove", post(remove_auto_rule))
        .route("/api/health", get(get_queue_health))
        .route("/api/auto-cleanup", get(get_auto_cleanup))
        .route("/api/auto-cleanup", post(set_auto_cleanup))
        .route("/api/dedup", get(get_dedup_config))
        .route("/api/dedup", post(set_dedup_config))
        .route("/api/conflict", get(get_conflict_strategy))
        .route("/api/conflict", post(set_conflict_strategy))
        .route("/api/conflict/check", post(check_conflict))
        .route("/api/domain-limit", get(get_domain_limit))
        .route("/api/domain-limit", post(set_domain_limit))
        .route("/api/protocol-limit", get(get_protocol_limit))
        .route("/api/protocol-limit", post(set_protocol_limit))
        .route("/api/path-validator", get(get_path_validator_config))
        .route("/api/path-validator", post(set_path_validator_config))
        .route("/api/path-validator/validate", post(validate_save_path))
        .route("/api/speed-history", get(get_all_speed_history))
        .route("/api/speed-history/:id", get(get_task_speed_history))
        .route(
            "/api/speed-history/:id/clear",
            post(clear_task_speed_history),
        )
        .route("/api/audit-log", get(get_audit_log))
        .route("/api/audit-log/clear", post(clear_audit_log))
        .route("/api/bandwidth-schedule", get(get_bandwidth_schedule))
        .route("/api/bandwidth-schedule", post(add_bandwidth_schedule_rule))
        .route(
            "/api/bandwidth-schedule/:id",
            post(remove_bandwidth_schedule_rule),
        )
        .route("/api/download-presets", get(list_download_presets))
        .route("/api/download-presets", post(add_download_preset))
        .route("/api/download-presets/:id", post(remove_download_preset))
        .route(
            "/api/download-presets/:id/apply/:task_id",
            post(apply_download_preset),
        )
        .route("/api/retry-policy", get(get_retry_policy))
        .route("/api/retry-policy", post(set_retry_policy))
        .route("/api/torrent-files/:task_id", get(get_torrent_files))
        .route("/api/tasks/:id/clone", post(clone_task))
        .route("/api/sequential-mode", get(get_sequential_mode))
        .route("/api/sequential-mode", post(set_sequential_mode))
        .route("/api/url-rewrite", get(get_url_rewrite_summary))
        .route("/api/url-rewrite/enable", post(set_url_rewrite_enabled))
        .route("/api/url-rewrite/rules", get(list_url_rewrite_rules))
        .route("/api/url-rewrite/rules", post(add_url_rewrite_rule))
        .route("/api/url-rewrite/rules/:id", post(remove_url_rewrite_rule))
        .route("/api/url-rewrite/preview", post(preview_url_rewrite))
        .route("/api/path-template", get(get_path_template_config))
        .route("/api/path-template", post(set_path_template))
        .route("/api/path-template/enable", post(set_path_template_enabled))
        .route("/api/path-template/preview", post(preview_path_template))
        .route("/api/data-cap", get(get_data_cap_status))
        .route("/api/data-cap", post(set_data_cap_config))
        .route("/api/data-cap/enable", post(set_data_cap_enabled))
        .route("/api/data-cap/reset", post(reset_data_cap_today))
        .route("/api/stats/download", get(get_download_stats))
        .route("/api/stats/download/reset", post(reset_download_stats))
        .route("/api/report/download", get(get_download_report))
        .route("/api/url-expander", get(get_url_expander))
        .route("/api/url-expander", post(set_url_expander))
        .route("/api/url-expander/expand", post(expand_url_handler))
        .route("/api/url-expander/validate", post(validate_url_handler))
        .route("/api/extract-links", post(extract_links_handler))
        .route(
            "/api/watch-folders",
            get(watch_folders_handler).post(add_watch_folder_handler),
        )
        .route(
            "/api/watch-folders/:id/remove",
            post(remove_watch_folder_handler),
        )
        .route(
            "/api/watch-folders/:id/enable",
            post(enable_watch_folder_handler),
        )
        .route(
            "/api/watch-folders/:id/disable",
            post(disable_watch_folder_handler),
        )
        .route("/api/watch-folders/scan", post(scan_watch_folders_handler))
        .route(
            "/api/path-rules",
            get(list_path_rules_handler).post(add_path_rule_handler),
        )
        .route("/api/path-rules/:id/remove", post(remove_path_rule_handler))
        .route("/api/path-rules/:id/enable", post(enable_path_rule_handler))
        .route(
            "/api/path-rules/:id/disable",
            post(disable_path_rule_handler),
        )
        .route("/api/path-rules/match", post(match_path_rule_handler))
        .route("/api/archive", get(get_archive_status))
        .route("/api/archive/list", post(list_archived_tasks))
        .route("/api/archive/:id/archive", post(archive_task_handler))
        .route(
            "/api/archive/:id/restore",
            post(restore_archived_task_handler),
        )
        .route(
            "/api/archive/:id/delete",
            post(delete_archived_task_handler),
        )
        .route("/api/archive/clear", post(clear_archive_handler))
        .route("/api/archive/config", post(set_archive_config_handler))
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

/// Clone (duplicate) an existing download task
async fn clone_task(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    match state.manager.clone_task(&id).await {
        Ok(new_id) => Json(TaskResponse {
            success: true,
            task_id: Some(new_id),
            message: "Task cloned successfully".to_string(),
        }),
        Err(e) => Json(TaskResponse {
            success: false,
            task_id: None,
            message: format!("Failed to clone task: {}", e),
        }),
    }
}

/// Request to set sequential download mode
#[derive(Debug, Deserialize)]
pub struct SequentialModeRequest {
    pub task_id: String,
    pub enabled: bool,
}

/// Request to enable/disable URL rewriting
#[derive(Debug, Deserialize)]
pub struct UrlRewriteEnabledRequest {
    pub enabled: bool,
}

/// Request to preview URL rewrite
#[derive(Debug, Deserialize)]
pub struct UrlRewritePreviewRequest {
    pub url: String,
}

/// Request to set path template
#[derive(Debug, Deserialize)]
pub struct PathTemplateRequest {
    pub template: String,
}

/// Request to preview path template
#[derive(Debug, Deserialize)]
pub struct PathTemplatePreviewRequest {
    pub template: String,
    pub filename: String,
    pub protocol: String,
}

/// Request to set data cap configuration
#[derive(Debug, Deserialize)]
pub struct DataCapConfigRequest {
    pub daily_limit_bytes: u64,
}

/// Request to enable/disable data cap
#[derive(Debug, Deserialize)]
pub struct DataCapEnabledRequest {
    pub enabled: bool,
}

/// Get sequential download mode for a task
async fn get_sequential_mode(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    if let Some(task_id) = params.get("task_id") {
        let enabled = state.manager.get_sequential_mode(task_id).await;
        Json(serde_json::json!({ "task_id": task_id, "sequential_mode": enabled }))
    } else {
        Json(serde_json::json!({ "error": "task_id parameter required" }))
    }
}

/// Set sequential download mode for a task
async fn set_sequential_mode(
    State(state): State<Arc<WebState>>,
    Json(req): Json<SequentialModeRequest>,
) -> Json<TaskResponse> {
    let success = state
        .manager
        .set_sequential_mode(&req.task_id, req.enabled)
        .await;
    Json(TaskResponse {
        success,
        task_id: Some(req.task_id),
        message: if success {
            "Sequential mode updated".to_string()
        } else {
            "Task not found".to_string()
        },
    })
}

/// Get URL rewrite summary (rules + stats)
async fn get_url_rewrite_summary(
    State(state): State<Arc<WebState>>,
) -> Json<crate::url_rewrite::UrlRewriteSummary> {
    let summary = state.manager.get_url_rewrite_summary().await;
    Json(summary)
}

/// Enable or disable URL rewriting globally
async fn set_url_rewrite_enabled(
    State(state): State<Arc<WebState>>,
    Json(req): Json<UrlRewriteEnabledRequest>,
) -> impl axum::response::IntoResponse {
    state.manager.set_url_rewrite_enabled(req.enabled).await;
    Json(serde_json::json!({"status": "ok", "enabled": req.enabled}))
}

/// List all URL rewrite rules
async fn list_url_rewrite_rules(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::url_rewrite::UrlRewriteRule>> {
    let rules = state.manager.list_url_rewrite_rules().await;
    Json(rules)
}

/// Add a URL rewrite rule
async fn add_url_rewrite_rule(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::url_rewrite::UrlRewriteRule>,
) -> impl axum::response::IntoResponse {
    state.manager.add_url_rewrite_rule(rule.clone()).await;
    Json(serde_json::json!({"status": "ok", "id": rule.id}))
}

/// Remove a URL rewrite rule
async fn remove_url_rewrite_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_url_rewrite_rule(&id).await;
    Json(serde_json::json!({"removed": removed}))
}

/// Preview URL rewrite without modifying apply counts
async fn preview_url_rewrite(
    State(state): State<Arc<WebState>>,
    Json(req): Json<UrlRewritePreviewRequest>,
) -> Json<serde_json::Value> {
    match state.manager.preview_url_rewrite(&req.url).await {
        Some((rewritten, rule_name)) => Json(serde_json::json!({
            "original": req.url,
            "rewritten": rewritten,
            "rule": rule_name,
            "matched": true
        })),
        None => Json(serde_json::json!({
            "original": req.url,
            "rewritten": req.url,
            "matched": false
        })),
    }
}

/// Get path template configuration
async fn get_path_template_config(
    State(state): State<Arc<WebState>>,
) -> Json<crate::path_template::PathTemplateConfig> {
    Json(state.manager.get_path_template_config().await)
}

/// Set path template
async fn set_path_template(
    State(state): State<Arc<WebState>>,
    Json(req): Json<PathTemplateRequest>,
) -> Json<serde_json::Value> {
    match state.manager.set_path_template(&req.template).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "message": "Path template set successfully"
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// Enable/disable path template
async fn set_path_template_enabled(
    State(state): State<Arc<WebState>>,
    Json(req): Json<UrlRewriteEnabledRequest>,
) -> Json<serde_json::Value> {
    state.manager.set_path_template_enabled(req.enabled).await;
    Json(serde_json::json!({
        "success": true,
        "enabled": req.enabled
    }))
}

/// Preview path template
/// Get data cap status
async fn get_data_cap_status(
    State(state): State<Arc<WebState>>,
) -> Json<crate::data_cap::DataCapStatus> {
    let status = state.manager.get_data_cap_status().await;
    Json(status)
}

/// Set data cap configuration
async fn set_data_cap_config(
    State(state): State<Arc<WebState>>,
    Json(req): Json<DataCapConfigRequest>,
) -> Json<serde_json::Value> {
    let config = crate::data_cap::DataCapConfig::new(true, req.daily_limit_bytes);
    state.manager.set_data_cap_config(config).await;
    Json(serde_json::json!({"success": true}))
}

/// Enable or disable data cap
async fn set_data_cap_enabled(
    State(state): State<Arc<WebState>>,
    Json(req): Json<DataCapEnabledRequest>,
) -> Json<serde_json::Value> {
    state.manager.set_data_cap_enabled(req.enabled).await;
    Json(serde_json::json!({"success": true}))
}

/// Reset today's data cap usage
async fn reset_data_cap_today(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.reset_data_cap_today().await;
    Json(serde_json::json!({"success": true}))
}

/// Get download statistics
async fn get_download_stats(
    State(state): State<Arc<WebState>>,
) -> Json<crate::download_stats::DownloadStatistics> {
    let stats = state.manager.get_download_stats().await;
    Json(stats)
}

/// Reset download statistics
async fn reset_download_stats(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.reset_download_stats().await;
    Json(serde_json::json!({"success": true}))
}

#[derive(Deserialize)]
struct ReportQuery {
    period: Option<String>,
}

async fn get_download_report(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(query): axum::extract::Query<ReportQuery>,
) -> Json<serde_json::Value> {
    use crate::download_report::{ReportConfig, ReportPeriod};
    let period = match query.period.as_deref() {
        Some("weekly") | Some("week") => ReportPeriod::Weekly,
        Some("monthly") | Some("month") => ReportPeriod::Monthly,
        _ => ReportPeriod::Daily,
    };
    let config = ReportConfig {
        period,
        ..Default::default()
    };
    let report = state.manager.generate_download_report(&config).await;
    let markdown = crate::download_report::format_report_markdown(&report, &config);
    Json(serde_json::json!({
        "success": true,
        "report": {
            "period": report.period.to_string(),
            "period_start": report.period_start.to_rfc3339(),
            "period_end": report.period_end.to_rfc3339(),
            "total_downloads": report.total_downloads,
            "completed_downloads": report.completed_downloads,
            "failed_downloads": report.failed_downloads,
            "total_bytes": report.total_bytes,
            "avg_speed_bps": report.avg_speed_bps,
            "success_rate": report.success_rate,
            "by_protocol": report.by_protocol,
            "top_by_size": report.top_by_size,
            "hourly": report.hourly,
        },
        "markdown": markdown
    }))
}

/// Get URL expander configuration
async fn get_url_expander(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_url_expander().await;
    Json(serde_json::json!({
        "success": true,
        "config": {
            "expansion_enabled": config.expansion_enabled,
            "validation_enabled": config.validation_enabled,
            "max_redirects": config.max_redirects,
            "timeout_secs": config.timeout_secs,
            "custom_shorteners": config.custom_shorteners,
            "block_on_unreachable": config.block_on_unreachable
        }
    }))
}

/// Set URL expander configuration
async fn set_url_expander(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut config = state.manager.get_url_expander().await;

    if let Some(v) = req.get("expansion_enabled").and_then(|v| v.as_bool()) {
        config.expansion_enabled = v;
    }
    if let Some(v) = req.get("validation_enabled").and_then(|v| v.as_bool()) {
        config.validation_enabled = v;
    }
    if let Some(v) = req.get("max_redirects").and_then(|v| v.as_u64()) {
        config.max_redirects = v as u32;
    }
    if let Some(v) = req.get("timeout_secs").and_then(|v| v.as_u64()) {
        config.timeout_secs = v;
    }
    if let Some(v) = req.get("block_on_unreachable").and_then(|v| v.as_bool()) {
        config.block_on_unreachable = v;
    }
    if let Some(arr) = req.get("custom_shorteners").and_then(|v| v.as_array()) {
        config.custom_shorteners = arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
    }

    state.manager.set_url_expander(config.clone()).await;

    Json(serde_json::json!({
        "success": true,
        "config": {
            "expansion_enabled": config.expansion_enabled,
            "validation_enabled": config.validation_enabled,
            "max_redirects": config.max_redirects,
            "timeout_secs": config.timeout_secs,
            "custom_shorteners": config.custom_shorteners,
            "block_on_unreachable": config.block_on_unreachable
        }
    }))
}

/// Expand a shortened URL
async fn expand_url_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let url = match req.get("url").and_then(|v| v.as_str()) {
        Some(u) => u,
        None => {
            return Json(serde_json::json!({
                "success": false,
                "error": "Missing 'url' field"
            }));
        }
    };

    let config = state.manager.get_url_expander().await;
    match crate::url_expander::expand_url(url, &config).await {
        Ok(result) => Json(serde_json::json!({
            "success": true,
            "original": result.original,
            "expanded": result.expanded,
            "was_expanded": result.was_expanded,
            "redirect_count": result.redirect_count
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// Validate a URL is reachable
async fn validate_url_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let url = match req.get("url").and_then(|v| v.as_str()) {
        Some(u) => u,
        None => {
            return Json(serde_json::json!({
                "success": false,
                "error": "Missing 'url' field"
            }));
        }
    };

    let config = state.manager.get_url_expander().await;
    match crate::url_expander::validate_url(url, &config).await {
        Ok(result) => Json(serde_json::json!({
            "success": true,
            "reachable": result.reachable,
            "status_code": result.status_code,
            "content_length": result.content_length,
            "content_type": result.content_type,
            "was_shortened": result.was_shortened,
            "final_url": result.final_url,
            "response_time_ms": result.response_time_ms,
            "error": result.error
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// Extract download links from HTML content or fetch a URL and extract links
async fn extract_links_handler(
    State(_state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    // Support two modes:
    // 1. { "url": "..." } - fetch the page and extract links
    // 2. { "html": "...", "base_url": "..." } - extract from provided HTML
    if let Some(url) = req.get("url").and_then(|v| v.as_str()) {
        match crate::DownloadManager::scrape_url_for_links(url).await {
            Ok(result) => Json(serde_json::json!({
                "success": true,
                "source_url": result.source_url,
                "links": result.links,
                "total": result.links.len(),
                "protocol_counts": result.protocol_counts
            })),
            Err(e) => Json(serde_json::json!({
                "success": false,
                "error": e
            })),
        }
    } else if let Some(html) = req.get("html").and_then(|v| v.as_str()) {
        let base_url = req.get("base_url").and_then(|v| v.as_str());
        let result = crate::DownloadManager::extract_links_from_html(html, base_url);
        Json(serde_json::json!({
            "success": true,
            "source_url": result.source_url,
            "links": result.links,
            "total": result.links.len(),
            "protocol_counts": result.protocol_counts
        }))
    } else {
        Json(serde_json::json!({
            "success": false,
            "error": "Provide 'url' to fetch a page or 'html' with optional 'base_url'"
        }))
    }
}

/// GET /api/watch-folders - List all watch folders
async fn watch_folders_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_watch_folder_summary().await;
    Json(serde_json::json!({
        "success": true,
        "summary": summary
    }))
}

/// POST /api/watch-folders - Add a new watch folder
async fn add_watch_folder_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let name = req
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("watch-folder")
        .to_string();
    let path = match req.get("path").and_then(|v| v.as_str()) {
        Some(p) => std::path::PathBuf::from(p),
        None => {
            return Json(serde_json::json!({
                "success": false,
                "error": "Missing 'path' field"
            }));
        }
    };
    let recursive = req
        .get("recursive")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let extensions: Vec<String> = req
        .get("extensions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let cleanup_after = req
        .get("cleanup_after")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let tags: Vec<String> = req
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let group = req
        .get("group")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    match state
        .manager
        .add_watch_folder(
            name,
            path,
            recursive,
            extensions,
            cleanup_after,
            tags,
            group,
        )
        .await
    {
        Ok(id) => Json(serde_json::json!({
            "success": true,
            "id": id,
            "message": "Watch folder added"
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// POST /api/watch-folders/:id/remove - Remove a watch folder
async fn remove_watch_folder_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.remove_watch_folder(&id).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "message": "Watch folder removed"
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// POST /api/watch-folders/:id/enable - Enable a watch folder
async fn enable_watch_folder_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.set_watch_folder_enabled(&id, true).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "message": "Watch folder enabled"
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// POST /api/watch-folders/:id/disable - Disable a watch folder
async fn disable_watch_folder_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.set_watch_folder_enabled(&id, false).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "message": "Watch folder disabled"
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

/// POST /api/watch-folders/scan - Scan all enabled watch folders
async fn scan_watch_folders_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let imported = state.manager.scan_watch_folders().await;
    Json(serde_json::json!({
        "success": true,
        "imported": imported,
        "message": format!("{} URL(s) imported", imported)
    }))
}

// ─── Phase 77: Path Rules REST API ───

async fn list_path_rules_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::path_rules::PathRule>> {
    Json(state.manager.list_path_rules().await)
}

#[derive(serde::Deserialize)]
struct AddPathRuleRequest {
    name: String,
    pattern_type: String, // "contains", "wildcard", "exact"
    pattern: String,
    save_path: String,
    #[serde(default)]
    match_url: bool,
    #[serde(default)]
    match_filename: bool,
    #[serde(default)]
    priority: u32,
}

async fn add_path_rule_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<AddPathRuleRequest>,
) -> Json<serde_json::Value> {
    let pattern = match req.pattern_type.to_lowercase().as_str() {
        "contains" => crate::path_rules::PathRulePattern::Contains(req.pattern),
        "wildcard" => crate::path_rules::PathRulePattern::Wildcard(req.pattern),
        "exact" => crate::path_rules::PathRulePattern::Exact(req.pattern),
        _ => {
            return Json(serde_json::json!({
                "success": false,
                "error": "Invalid pattern_type. Use: contains, wildcard, exact"
            }));
        }
    };

    let id = format!("prule_{}", chrono::Utc::now().timestamp_millis());
    let rule = crate::path_rules::PathRule {
        id: id.clone(),
        name: req.name,
        pattern,
        match_url: req.match_url,
        match_filename: req.match_filename,
        save_path: std::path::PathBuf::from(req.save_path),
        enabled: true,
        priority: req.priority,
    };

    match state.manager.add_path_rule(rule).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "id": id,
            "message": "Path rule added"
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

async fn remove_path_rule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.remove_path_rule(&id).await {
        Ok(_) => Json(serde_json::json!({
            "success": true,
            "message": format!("Path rule {} removed", id)
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

async fn enable_path_rule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.set_path_rule_enabled(&id, true).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "message": format!("Path rule {} enabled", id)
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

async fn disable_path_rule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.set_path_rule_enabled(&id, false).await {
        Ok(()) => Json(serde_json::json!({
            "success": true,
            "message": format!("Path rule {} disabled", id)
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
}

#[derive(serde::Deserialize)]
struct MatchPathRuleRequest {
    url: String,
    filename: String,
}

async fn match_path_rule_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<MatchPathRuleRequest>,
) -> Json<serde_json::Value> {
    match state
        .manager
        .find_matching_path_rule(&req.url, &req.filename)
        .await
    {
        Some(rule) => Json(serde_json::json!({
            "matched": true,
            "rule": {
                "id": rule.id,
                "name": rule.name,
                "save_path": rule.save_path.display().to_string(),
                "priority": rule.priority,
            }
        })),
        None => Json(serde_json::json!({
            "matched": false,
            "message": "No matching path rule found"
        })),
    }
}

async fn preview_path_template(
    State(_state): State<Arc<WebState>>,
    Json(req): Json<PathTemplatePreviewRequest>,
) -> Json<serde_json::Value> {
    match crate::DownloadManager::preview_path_template(&req.template, &req.filename, &req.protocol)
    {
        Ok(path) => Json(serde_json::json!({
            "success": true,
            "template": req.template,
            "filename": req.filename,
            "protocol": req.protocol,
            "path": path
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })),
    }
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

/// Get queue health report
async fn get_queue_health(
    State(state): State<Arc<WebState>>,
) -> Json<crate::queue_health::QueueHealthReport> {
    let config = crate::queue_health::HealthMonitorConfig::default();
    let report = state.manager.get_queue_health_report(&config).await;
    Json(report)
}

/// Get speed history summary for all tasks
async fn get_all_speed_history(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::speed_history::SpeedHistorySummary>> {
    let summaries = state.manager.get_all_speed_history_summaries().await;
    Json(summaries)
}

/// Get auto-cleanup configuration
async fn get_auto_cleanup(
    State(state): State<Arc<WebState>>,
) -> Json<crate::auto_cleanup::AutoCleanupConfig> {
    let config = state.manager.get_auto_cleanup().await;
    Json(config)
}

/// Set auto-cleanup configuration
async fn set_auto_cleanup(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::auto_cleanup::AutoCleanupConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_auto_cleanup(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get archive status summary
async fn get_archive_status(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_archive_summary().await;
    let config = state.manager.get_archive_config().await;
    Json(serde_json::json!({
        "config": config,
        "summary": summary
    }))
}

/// List archived tasks with optional filters
async fn list_archived_tasks(
    State(state): State<Arc<WebState>>,
    Json(filter): Json<serde_json::Value>,
) -> Json<Vec<crate::task_archive::ArchivedTask>> {
    let state_filter = filter
        .get("state")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let protocol_filter = filter
        .get("protocol")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tag_filter = filter
        .get("tag")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let archived = state
        .manager
        .list_archived_tasks(
            state_filter.as_deref(),
            protocol_filter.as_deref(),
            tag_filter.as_deref(),
        )
        .await;
    Json(archived)
}

/// Archive a task
async fn archive_task_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let reason = body
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    match state.manager.archive_task(&id, reason).await {
        Ok(()) => (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"status": "ok"})),
        ),
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// Restore an archived task
async fn restore_archived_task_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.restore_archived_task(&id).await {
        Ok(new_id) => (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"status": "ok", "new_id": new_id})),
        ),
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// Delete an archived task permanently
async fn delete_archived_task_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    if state.manager.delete_archived_task(&id).await {
        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"status": "ok"})),
        )
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Task not found"})),
        )
    }
}

/// Clear all archived tasks
async fn clear_archive_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_archive().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Set archive configuration
async fn set_archive_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_archive::ArchiveConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_archive_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get URL deduplication configuration
async fn get_dedup_config(
    State(state): State<Arc<WebState>>,
) -> Json<crate::url_dedup::DedupConfig> {
    let config = state.manager.get_url_dedup().await;
    Json(config)
}

/// Set URL deduplication configuration
async fn set_dedup_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::url_dedup::DedupConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_url_dedup(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get conflict detection strategy
async fn get_conflict_strategy(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let strategy = state.manager.get_conflict_strategy().await;
    Json(serde_json::json!({
        "strategy": strategy.to_string()
    }))
}

/// Set conflict detection strategy
async fn set_conflict_strategy(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let strategy_str = body
        .get("strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("skip");
    match strategy_str.parse::<crate::conflict_detection::ConflictStrategy>() {
        Ok(strategy) => {
            state.manager.set_conflict_strategy(strategy).await;
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({"status": "ok"})),
            )
        }
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

/// Check for file path conflicts
async fn check_conflict(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = body
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("new");
    let task_name = body
        .get("task_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let save_path_str = body.get("save_path").and_then(|v| v.as_str()).unwrap_or("");
    let save_path = std::path::PathBuf::from(save_path_str);
    let report = state
        .manager
        .check_conflicts(task_id, task_name, &save_path)
        .await;
    Json(serde_json::json!({
        "task_id": report.task_id,
        "task_name": report.task_name,
        "target_path": report.target_path.to_string_lossy(),
        "resolved_path": report.resolved_path.to_string_lossy(),
        "action": format!("{:?}", report.action),
        "conflict": report.conflict.map(|c| format!("{:?}", c))
    }))
}

/// Get per-domain download limit configuration and status
async fn get_domain_limit(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_domain_limit_summary().await;
    Json(
        serde_json::to_value(&summary)
            .unwrap_or_else(|_| serde_json::json!({"error": "serialization failed"})),
    )
}

/// Set per-domain download limit configuration
async fn set_domain_limit(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let mut config = state.manager.get_domain_limit_config().await;

    // Update enabled flag if provided
    if let Some(enabled) = body.get("enabled").and_then(|v| v.as_bool()) {
        config.enabled = enabled;
    }

    // Update default_limit if provided
    if let Some(limit) = body.get("default_limit").and_then(|v| v.as_u64()) {
        config.default_limit = limit as u32;
    }

    // Add domain override if provided
    if let Some(domain) = body.get("domain").and_then(|v| v.as_str())
        && let Some(limit) = body.get("limit").and_then(|v| v.as_u64())
    {
        config.set_domain_limit(domain, limit as u32);
    }

    // Remove domain override if requested
    if let Some(domain) = body.get("remove_domain").and_then(|v| v.as_str()) {
        config.remove_domain_limit(domain);
    }

    state.manager.set_domain_limit_config(config).await;
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({"status": "ok"})),
    )
}

/// Get per-protocol download limit configuration
async fn get_protocol_limit(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_protocol_limits_summary().await;
    Json(
        serde_json::to_value(&summary)
            .unwrap_or_else(|_| serde_json::json!({"error": "serialization failed"})),
    )
}

/// Set per-protocol download limit configuration
async fn set_protocol_limit(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let mut config = state.manager.get_protocol_limits().await;

    // Update enabled flag if provided
    if let Some(enabled) = body.get("enabled").and_then(|v| v.as_bool()) {
        config.enabled = enabled;
    }

    // Update default_max_concurrent if provided
    if let Some(limit) = body.get("default_max_concurrent").and_then(|v| v.as_u64()) {
        config.default_max_concurrent = limit as u32;
    }

    // Add protocol override if provided
    if let Some(protocol_str) = body.get("protocol").and_then(|v| v.as_str())
        && let Some(limit) = body.get("limit").and_then(|v| v.as_u64())
        && let Some(protocol) = crate::protocol_limits::key_to_protocol(protocol_str)
    {
        config.set_limit(protocol, limit as u32, true);
    }

    // Remove protocol override if requested
    if let Some(protocol_str) = body.get("remove_protocol").and_then(|v| v.as_str())
        && let Some(protocol) = crate::protocol_limits::key_to_protocol(protocol_str)
    {
        config.remove_limit(protocol);
    }

    state.manager.set_protocol_limits(config).await;
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({"status": "ok"})),
    )
}

/// Get path validator configuration
async fn get_path_validator_config(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_path_validator_config().await;
    Json(
        serde_json::to_value(&config)
            .unwrap_or_else(|_| serde_json::json!({"error": "serialization failed"})),
    )
}

/// Set path validator configuration
async fn set_path_validator_config(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let mut config = state.manager.get_path_validator_config().await;

    if let Some(auto_create) = body.get("auto_create_dirs").and_then(|v| v.as_bool()) {
        config.auto_create_dirs = auto_create;
    }
    if let Some(max_len) = body.get("max_path_length").and_then(|v| v.as_u64()) {
        config.max_path_length = max_len as usize;
    }
    if let Some(check_reserved) = body.get("check_reserved_names").and_then(|v| v.as_bool()) {
        config.check_reserved_names = check_reserved;
    }
    if let Some(allow_abs) = body.get("allow_absolute_paths").and_then(|v| v.as_bool()) {
        config.allow_absolute_paths = allow_abs;
    }
    if let Some(base_dir) = body.get("base_dir").and_then(|v| v.as_str()) {
        config.base_dir = std::path::PathBuf::from(base_dir);
    }

    state.manager.set_path_validator_config(config).await;
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({"status": "ok"})),
    )
}

/// Validate a save path
async fn validate_save_path(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let path = body.get("path").and_then(|v| v.as_str()).unwrap_or("");
    if path.is_empty() {
        return Json(serde_json::json!({"error": "path is required"}));
    }
    let result = state.manager.validate_save_path(path).await;
    Json(serde_json::json!({
        "is_valid": result.is_valid,
        "canonical_path": result.canonical_path.map(|p| p.to_string_lossy().to_string()),
        "warnings": result.warnings,
        "error": result.error,
    }))
}

/// Get speed history for a specific task
async fn get_task_speed_history(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_task_speed_history(&task_id).await;
    match summary {
        Some(s) => Json(serde_json::json!({
            "found": true,
            "summary": s,
        }))
        .into_response(),
        None => Json(serde_json::json!({
            "found": false,
            "summary": null,
        }))
        .into_response(),
    }
}

/// Clear speed history for a task
async fn clear_task_speed_history(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.clear_task_speed_history(&task_id).await;
    Json(serde_json::json!({ "cleared": removed }))
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

/// GET /api/task-speed?task_id=xxx
/// Returns the per-task speed limit for a specific task.
async fn get_task_speed(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let task_id = match params.get("task_id") {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "task_id parameter required"})),
            );
        }
    };
    let limit = state.manager.get_task_speed_limit(task_id).await;
    (
        StatusCode::OK,
        Json(serde_json::json!({"task_id": task_id, "speed_limit_bps": limit})),
    )
}

/// POST /api/task-speed
/// Set per-task speed limit. Body: {"task_id": "xxx", "speed_limit_bps": 102400}
async fn set_task_speed(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let task_id = match body.get("task_id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "task_id required"})),
            );
        }
    };
    let speed_limit = body
        .get("speed_limit_bps")
        .and_then(|v| v.as_u64())
        .filter(|&v| v != 0);
    state
        .manager
        .set_task_speed_limit_per_task(&task_id, speed_limit)
        .await;
    let limit_str = match speed_limit {
        Some(bps) => format!("{:.1} KB/s", bps as f64 / 1024.0),
        None => "unlimited (global default)".to_string(),
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({"success": true, "task_id": task_id, "speed_limit": limit_str})),
    )
}

/// POST /api/checksum
/// Body: {"task_id": "xxx", "checksum": "hex_hash", "algorithm": "sha256"}
async fn set_checksum(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let task_id = match body.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "task_id is required"})),
            );
        }
    };
    let checksum = match body.get("checksum").and_then(|v| v.as_str()) {
        Some(cs) if !cs.is_empty() => cs.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "checksum is required"})),
            );
        }
    };
    let algo_str = body.get("algorithm").and_then(|v| v.as_str()).unwrap_or("");

    // Auto-detect algorithm if not specified
    let algo = if !algo_str.is_empty() {
        match crate::checksum::ChecksumAlgorithm::parse(algo_str) {
            Some(a) => a,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("Unknown algorithm: {}", algo_str)})),
                );
            }
        }
    } else {
        match crate::checksum::detect_algorithm(&checksum) {
            Some(a) => a,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        serde_json::json!({"error": "Cannot auto-detect algorithm from checksum length"}),
                    ),
                );
            }
        }
    };

    match state
        .manager
        .set_task_checksum(&task_id, &checksum, algo)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(
                serde_json::json!({"success": true, "task_id": task_id, "algorithm": algo.name(), "checksum": checksum.to_lowercase()}),
            ),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

/// GET /api/hooks - List all post-download hooks
async fn list_hooks(State(state): State<Arc<WebState>>) -> Json<Vec<crate::post_hooks::PostHook>> {
    Json(state.manager.hook_manager().list_hooks())
}

/// POST /api/hooks - Add a new post-download hook
async fn add_hook(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "name is required"})),
            );
        }
    };
    let command = match body.get("command").and_then(|v| v.as_str()) {
        Some(c) if !c.is_empty() => c.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "command is required"})),
            );
        }
    };
    let event_str = body
        .get("event")
        .and_then(|v| v.as_str())
        .unwrap_or("on_complete");
    let event = match event_str {
        "on_complete" => crate::post_hooks::HookEvent::OnComplete,
        "on_failure" => crate::post_hooks::HookEvent::OnFailure,
        "both" => crate::post_hooks::HookEvent::Both,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid event type"})),
            );
        }
    };

    let mut hook = crate::post_hooks::PostHook::new(name, event, command);
    if let Some(timeout) = body.get("timeout_secs").and_then(|v| v.as_u64()) {
        hook = hook.with_timeout(timeout);
    }

    match state.manager.hook_manager().add_hook(hook) {
        Ok(hook_id) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"success": true, "hook_id": hook_id})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/hooks/:id - Update a hook
async fn update_hook(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let existing = match state.manager.hook_manager().get_hook(&id) {
        Some(h) => h,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Hook not found"})),
            );
        }
    };

    let mut updated = existing;
    if let Some(name) = body.get("name").and_then(|v| v.as_str()) {
        updated.name = name.to_string();
    }
    if let Some(command) = body.get("command").and_then(|v| v.as_str()) {
        updated.command = command.to_string();
    }
    if let Some(event_str) = body.get("event").and_then(|v| v.as_str()) {
        updated.event = match event_str {
            "on_complete" => crate::post_hooks::HookEvent::OnComplete,
            "on_failure" => crate::post_hooks::HookEvent::OnFailure,
            "both" => crate::post_hooks::HookEvent::Both,
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "Invalid event type"})),
                );
            }
        };
    }
    if let Some(timeout) = body.get("timeout_secs").and_then(|v| v.as_u64()) {
        updated.timeout_secs = timeout;
    }
    if let Some(enabled) = body.get("enabled").and_then(|v| v.as_bool()) {
        updated.enabled = enabled;
    }

    match state.manager.hook_manager().update_hook(&id, updated) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Hook not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/hooks/:id/remove - Remove a hook
async fn remove_hook(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.manager.hook_manager().remove_hook(&id) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Hook not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// GET /api/feeds - List all RSS feed subscriptions
async fn list_feeds(State(state): State<Arc<WebState>>) -> (StatusCode, Json<serde_json::Value>) {
    let rss_mgr = match state.manager.rss_feed_manager() {
        Some(mgr) => mgr,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "RSS feed manager not initialized"})),
            );
        }
    };
    let subs = rss_mgr.list().await;
    (StatusCode::OK, Json(serde_json::json!(subs)))
}

/// POST /api/feeds - Add a new RSS feed subscription
async fn add_feed(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let rss_mgr = match state.manager.rss_feed_manager() {
        Some(mgr) => mgr,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "RSS feed manager not initialized"})),
            );
        }
    };

    let feed_url = match body.get("feed_url").and_then(|v| v.as_str()) {
        Some(url) if !url.is_empty() => url,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "feed_url is required"})),
            );
        }
    };
    let label = body.get("label").and_then(|v| v.as_str());
    let title_filter = body.get("title_filter").and_then(|v| v.as_str());
    let extensions: Vec<String> = body
        .get("extensions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    match rss_mgr
        .add_subscription(feed_url, label, title_filter, extensions)
        .await
    {
        Ok(id) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"success": true, "id": id})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/feeds/:id/remove - Remove an RSS feed subscription
async fn remove_feed(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let rss_mgr = match state.manager.rss_feed_manager() {
        Some(mgr) => mgr,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "RSS feed manager not initialized"})),
            );
        }
    };
    match rss_mgr.remove_subscription(&id).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/feeds/:id/enable - Enable an RSS feed subscription
async fn enable_feed(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let rss_mgr = match state.manager.rss_feed_manager() {
        Some(mgr) => mgr,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "RSS feed manager not initialized"})),
            );
        }
    };
    match rss_mgr.set_enabled(&id, true).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/feeds/:id/disable - Disable an RSS feed subscription
async fn disable_feed(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let rss_mgr = match state.manager.rss_feed_manager() {
        Some(mgr) => mgr,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "RSS feed manager not initialized"})),
            );
        }
    };
    match rss_mgr.set_enabled(&id, false).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/feeds/:id/poll - Poll an RSS feed for new items
async fn poll_feed(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let rss_mgr = match state.manager.rss_feed_manager() {
        Some(mgr) => mgr,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "RSS feed manager not initialized"})),
            );
        }
    };
    match rss_mgr.poll_feed(&id).await {
        Ok(items) => (StatusCode::OK, Json(serde_json::json!({"items": items}))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// GET /api/eta — Get ETA estimates for all active downloads
async fn get_all_eta(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let tasks = state.manager.list_tasks().await;
    let mut results = Vec::new();
    for task in &tasks {
        if task.state == crate::DownloadState::Downloading && task.speed_bps > 0.0 {
            let remaining = task.size.saturating_sub(task.downloaded);
            if let Some(estimate) = state
                .manager
                .eta_estimator()
                .estimate(&task.id, remaining)
                .await
            {
                results.push(serde_json::json!({
                    "task_id": task.id,
                    "task_name": task.name,
                    "estimated_secs": estimate.estimated_secs,
                    "optimistic_secs": estimate.optimistic_secs,
                    "pessimistic_secs": estimate.pessimistic_secs,
                    "confidence": estimate.confidence.label(),
                    "smoothed_speed_bps": estimate.smoothed_speed_bps,
                    "raw_speed_bps": estimate.raw_speed_bps,
                    "sample_count": estimate.sample_count,
                    "formatted": estimate.format_eta(),
                    "range": estimate.format_range()
                }));
            }
        }
    }
    Json(serde_json::json!({"eta_estimates": results}))
}

/// GET /api/eta/:id — Get ETA estimate for a specific task
async fn get_task_eta(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let task = state
        .manager
        .get_task(&id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    if task.state != crate::DownloadState::Downloading || task.speed_bps <= 0.0 {
        return Ok(Json(
            serde_json::json!({"error": "Task not actively downloading"}),
        ));
    }
    let remaining = task.size.saturating_sub(task.downloaded);
    match state
        .manager
        .eta_estimator()
        .estimate(&task.id, remaining)
        .await
    {
        Some(estimate) => Ok(Json(serde_json::json!({
            "task_id": task.id,
            "task_name": task.name,
            "estimated_secs": estimate.estimated_secs,
            "optimistic_secs": estimate.optimistic_secs,
            "pessimistic_secs": estimate.pessimistic_secs,
            "confidence": estimate.confidence.label(),
            "smoothed_speed_bps": estimate.smoothed_speed_bps,
            "raw_speed_bps": estimate.raw_speed_bps,
            "sample_count": estimate.sample_count,
            "formatted": estimate.format_eta(),
            "range": estimate.format_range()
        }))),
        None => Ok(Json(
            serde_json::json!({"error": "Insufficient data for ETA estimate"}),
        )),
    }
}

/// Request to add an auto-categorization rule
#[derive(Debug, Deserialize)]
struct AddAutoRuleRequest {
    name: String,
    pattern: String,
    pattern_type: String, // "contains", "wildcard", "exact"
    match_url: bool,
    match_filename: bool,
    tags: Vec<String>,
    group: Option<String>,
    priority: u32,
}

/// List all auto-categorization rules
async fn list_auto_rules(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let rules = state.manager.list_categorize_rules().await;
    let rules_json: Vec<serde_json::Value> = rules
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "name": r.name,
                "pattern": match &r.pattern {
                    crate::auto_categorize::CategorizePattern::Contains(s) => serde_json::json!({"type": "contains", "value": s}),
                    crate::auto_categorize::CategorizePattern::Wildcard(s) => serde_json::json!({"type": "wildcard", "value": s}),
                    crate::auto_categorize::CategorizePattern::Exact(s) => serde_json::json!({"type": "exact", "value": s}),
                },
                "match_url": r.match_url,
                "match_filename": r.match_filename,
                "tags": r.action.tags,
                "group": r.action.group,
                "enabled": r.enabled,
                "priority": r.priority,
            })
        })
        .collect();
    Json(serde_json::json!({"rules": rules_json}))
}

/// Add a new auto-categorization rule
async fn add_auto_rule(
    State(state): State<Arc<WebState>>,
    Json(req): Json<AddAutoRuleRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pattern = match req.pattern_type.as_str() {
        "contains" => crate::auto_categorize::CategorizePattern::Contains(req.pattern),
        "wildcard" => crate::auto_categorize::CategorizePattern::Wildcard(req.pattern),
        "exact" => crate::auto_categorize::CategorizePattern::Exact(req.pattern),
        _ => {
            return Ok(Json(
                serde_json::json!({"error": "Invalid pattern_type. Use 'contains', 'wildcard', or 'exact'"}),
            ));
        }
    };

    let rule = crate::auto_categorize::CategorizeRule {
        id: uuid::Uuid::new_v4().to_string(),
        name: req.name,
        pattern,
        match_url: req.match_url,
        match_filename: req.match_filename,
        action: crate::auto_categorize::CategorizeAction {
            tags: req.tags,
            group: req.group,
        },
        enabled: true,
        priority: req.priority,
    };

    match state.manager.add_categorize_rule(rule).await {
        Ok(()) => Ok(Json(
            serde_json::json!({"success": true, "message": "Rule added"}),
        )),
        Err(e) => Ok(Json(
            serde_json::json!({"error": format!("Failed to add rule: {}", e)}),
        )),
    }
}

/// Remove an auto-categorization rule
async fn remove_auto_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    if state.manager.remove_categorize_rule(&id).await {
        Json(serde_json::json!({"success": true, "message": "Rule removed"}))
    } else {
        Json(serde_json::json!({"error": "Rule not found"}))
    }
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

/// Get audit log entries
async fn get_audit_log(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let entries = state.manager.get_recent_audit_entries(100).await;
    let summary = state.manager.get_audit_summary().await;
    Json(serde_json::json!({
        "entries": entries,
        "summary": summary,
        "total": entries.len()
    }))
}

/// Clear audit log
async fn clear_audit_log(State(state): State<Arc<WebState>>) -> Json<TaskResponse> {
    state.manager.clear_audit_log().await;
    Json(TaskResponse {
        success: true,
        message: "Audit log cleared".to_string(),
        task_id: None,
    })
}

/// Get bandwidth schedule rules
async fn get_bandwidth_schedule(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::BandwidthScheduleRule>> {
    let rules = state.manager.list_bandwidth_schedule_rules().await;
    Json(rules)
}

/// Add a bandwidth schedule rule
async fn add_bandwidth_schedule_rule(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::BandwidthScheduleRule>,
) -> Json<TaskResponse> {
    state
        .manager
        .add_bandwidth_schedule_rule(rule.clone())
        .await;
    Json(TaskResponse {
        success: true,
        message: format!("Added bandwidth schedule rule: {}", rule.name),
        task_id: Some(rule.id),
    })
}

/// Remove a bandwidth schedule rule
async fn remove_bandwidth_schedule_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let removed = state.manager.remove_bandwidth_schedule_rule(&id).await;
    Json(TaskResponse {
        success: removed,
        message: if removed {
            format!("Removed rule {}", id)
        } else {
            format!("Rule {} not found", id)
        },
        task_id: None,
    })
}

/// List all download presets
async fn list_download_presets(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::download_presets::DownloadPreset>> {
    let presets = state.manager.list_download_presets().await;
    Json(presets)
}

/// Add a download preset
async fn add_download_preset(
    State(state): State<Arc<WebState>>,
    Json(preset): Json<crate::download_presets::DownloadPreset>,
) -> Json<TaskResponse> {
    state.manager.add_download_preset(preset.clone()).await;
    Json(TaskResponse {
        success: true,
        message: format!("Added download preset: {}", preset.name),
        task_id: Some(preset.id),
    })
}

/// Remove a download preset
async fn remove_download_preset(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let removed = state.manager.remove_download_preset(&id).await;
    Json(TaskResponse {
        success: removed,
        message: if removed {
            format!("Removed preset {}", id)
        } else {
            format!("Preset {} not found", id)
        },
        task_id: None,
    })
}

/// Apply a preset to a task
async fn apply_download_preset(
    State(state): State<Arc<WebState>>,
    axum::extract::Path((preset_id, task_id)): axum::extract::Path<(String, String)>,
) -> Json<TaskResponse> {
    let applied = state
        .manager
        .apply_preset_to_task(&task_id, &preset_id)
        .await;
    Json(TaskResponse {
        success: applied,
        message: if applied {
            format!("Applied preset {} to task {}", preset_id, task_id)
        } else {
            format!("Failed to apply preset (not found, disabled, or task not found)")
        },
        task_id: Some(task_id),
    })
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

/// Request to set per-task retry policy
#[derive(Debug, Deserialize)]
struct SetRetryPolicyRequest {
    task_id: String,
    /// None to clear (use global defaults)
    max_retries: Option<u32>,
    /// "fixed", "exponential", or "linear"
    backoff_type: Option<String>,
    /// Base delay in seconds (for exponential/linear) or fixed delay
    base_secs: Option<u64>,
}

/// GET /api/retry-policy?task_id=xxx
async fn get_retry_policy(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let task_id = params.get("task_id").ok_or(StatusCode::BAD_REQUEST)?;
    let policy = state.manager.get_task_retry_policy(task_id).await;
    Ok(Json(match policy {
        Some(p) => serde_json::json!({
            "task_id": task_id,
            "max_retries": p.max_retries,
            "backoff_type": match p.backoff {
                crate::RetryBackoff::Fixed(_) => "fixed",
                crate::RetryBackoff::Exponential { .. } => "exponential",
                crate::RetryBackoff::Linear { .. } => "linear",
            },
            "base_secs": match p.backoff {
                crate::RetryBackoff::Fixed(s) => s,
                crate::RetryBackoff::Exponential { base_secs } => base_secs,
                crate::RetryBackoff::Linear { base_secs } => base_secs,
            },
        }),
        None => serde_json::json!({
            "task_id": task_id,
            "max_retries": null,
            "backoff_type": null,
            "base_secs": null,
        }),
    }))
}

/// POST /api/retry-policy
async fn set_retry_policy(
    State(state): State<Arc<WebState>>,
    Json(req): Json<SetRetryPolicyRequest>,
) -> Result<Json<TaskResponse>, StatusCode> {
    let policy = match (req.max_retries, req.backoff_type, req.base_secs) {
        (None, _, _) => None,
        (Some(max_retries), backoff_type, base_secs) => {
            let backoff = match backoff_type.as_deref() {
                Some("fixed") => crate::RetryBackoff::Fixed(base_secs.unwrap_or(60)),
                Some("linear") => crate::RetryBackoff::Linear {
                    base_secs: base_secs.unwrap_or(30),
                },
                _ => crate::RetryBackoff::Exponential {
                    base_secs: base_secs.unwrap_or(30),
                },
            };
            Some(crate::RetryPolicy {
                max_retries,
                backoff,
            })
        }
    };

    if state
        .manager
        .set_task_retry_policy(&req.task_id, policy)
        .await
    {
        Ok(Json(TaskResponse {
            success: true,
            task_id: Some(req.task_id.clone()),
            message: "Retry policy updated".to_string(),
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// Response for torrent file listing
#[derive(Debug, Serialize)]
struct TorrentFilesResponse {
    task_id: String,
    files: Vec<crate::torrent::FileEntry>,
    total_size: u64,
    selected_size: u64,
}

/// GET /api/torrent-files/:task_id - List files in a multi-file torrent
async fn get_torrent_files(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<TorrentFilesResponse>, StatusCode> {
    // Look for torrent file in data_dir/torrents/<task_id>.torrent
    let data_dir = state.manager.data_dir();
    let torrent_path = data_dir
        .join("torrents")
        .join(format!("{}.torrent", task_id));

    if !torrent_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let selection = crate::torrent::FileSelection::all();
    match state
        .manager
        .inspect_torrent_files(&torrent_path, &selection)
        .await
    {
        Ok(files) => {
            let total_size: u64 = files.iter().map(|f| f.size).sum();
            let selected_size: u64 = files.iter().filter(|f| f.selected).map(|f| f.size).sum();
            Ok(Json(TorrentFilesResponse {
                task_id,
                files,
                total_size,
                selected_size,
            }))
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
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
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                checksum_status: None,
                eta_seconds: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
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
    async fn test_get_queue_health() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let report: crate::queue_health::QueueHealthReport = serde_json::from_slice(&body).unwrap();
        assert_eq!(report.summary.total_tasks, 0);
        assert_eq!(report.summary.health_score, 100);
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
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            current_session_start: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let info = TaskInfo::from(task);
        assert_eq!(info.depends_on, vec!["task-0".to_string()]);
    }

    #[tokio::test]
    async fn test_list_feeds_no_rss_manager() {
        // Without init_rss_feed_manager, rss_feed_manager is None
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/feeds")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_add_feed_no_rss_manager() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/feeds")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "feed_url": "https://example.com/feed.xml"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_list_feeds_empty() {
        let state = test_state_with_rss().await;
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/feeds")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let feeds: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert!(feeds.is_empty());
    }

    #[tokio::test]
    async fn test_add_and_list_feed() {
        let state = test_state_with_rss().await;
        let app = create_router(state.clone());

        // Add a feed
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/feeds")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "feed_url": "https://example.com/feed.xml",
                            "label": "Test Feed",
                            "extensions": ["mp4", "mkv"]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp["success"], true);
        assert!(resp["id"].as_str().is_some());

        // List feeds - should have 1
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/feeds")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let feeds: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0]["feed_url"], "https://example.com/feed.xml");
        assert_eq!(feeds[0]["label"], "Test Feed");
    }

    #[tokio::test]
    async fn test_add_feed_missing_url() {
        let state = test_state_with_rss().await;
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/feeds")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "label": "No URL"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_remove_feed_not_found() {
        let state = test_state_with_rss().await;
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/feeds/nonexistent-id/remove")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_enable_disable_feed() {
        let state = test_state_with_rss().await;
        let app = create_router(state.clone());

        // Add a feed first
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/feeds")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "feed_url": "https://example.com/feed.xml"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let feed_id = resp["id"].as_str().unwrap().to_string();

        // Disable it
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/feeds/{}/disable", feed_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify disabled
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/feeds")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let feeds: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(feeds[0]["enabled"], false);

        // Re-enable
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/feeds/{}/enable", feed_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn test_state_with_rss() -> Arc<WebState> {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let mut manager = DownloadManager::new(data_dir);
        manager.init_rss_feed_manager().await.unwrap();
        // Leak the tempdir so it lives for the test duration
        std::mem::forget(tmp);
        Arc::new(WebState::new(Arc::new(manager)))
    }

    #[tokio::test]
    async fn test_get_all_eta_empty() {
        let state = test_state();
        let app = Router::new()
            .route("/api/eta", get(get_all_eta))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/eta")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["eta_estimates"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_task_eta_not_found() {
        let state = test_state();
        let app = Router::new()
            .route("/api/eta/:id", get(get_task_eta))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/eta/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_path_template_config() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/path-template")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_set_path_template() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/path-template")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"template":"{category}/{name}"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
    }

    #[tokio::test]
    async fn test_set_path_template_invalid() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/path-template")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"template":"{unknown}"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], false);
    }

    #[tokio::test]
    async fn test_path_template_enable_disable() {
        let state = test_state();
        let app = create_router(state.clone());

        // Enable
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/path-template/enable")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"enabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify enabled
        let config = state.manager.get_path_template_config().await;
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_preview_path_template() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/path-template/preview")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"template":"{category}/{name}","filename":"movie.mp4","protocol":"http"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["path"], "video/movie");
    }

    #[tokio::test]
    async fn test_get_data_cap_status() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/data-cap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["config"]["enabled"], false);
        assert_eq!(json["cap_reached"], false);
    }

    #[tokio::test]
    async fn test_set_data_cap_config() {
        let state = test_state();
        let app = create_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/data-cap")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"daily_limit_bytes":1073741824}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);

        // Verify it took effect
        let status = state.manager.get_data_cap_status().await;
        assert_eq!(status.config.daily_limit_bytes, 1073741824);
    }

    #[tokio::test]
    async fn test_set_data_cap_enabled() {
        let state = test_state();
        let app = create_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/data-cap/enable")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"enabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status = state.manager.get_data_cap_status().await;
        assert_eq!(status.config.enabled, true);
    }

    #[tokio::test]
    async fn test_reset_data_cap_today() {
        let state = test_state();

        // Set a limit and record some usage
        state.manager.set_data_cap_limit(1000).await;
        state.manager.set_data_cap_enabled(true).await;
        state.manager.record_data_cap_usage("task-1", 500).await;

        let app = create_router(state.clone());

        // Reset
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/data-cap/reset")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status = state.manager.get_data_cap_status().await;
        assert_eq!(status.today_usage.bytes_downloaded, 0);
    }

    #[tokio::test]
    async fn test_get_download_stats() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/stats/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("total_downloads").is_some());
        assert!(json.get("total_completed").is_some());
        assert!(json.get("total_failed").is_some());
    }

    #[tokio::test]
    async fn test_reset_download_stats() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/stats/download/reset")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
    }
}
