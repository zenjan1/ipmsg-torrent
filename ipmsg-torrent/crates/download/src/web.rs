//! Web UI for download management
//!
//! Provides a REST API, WebSocket real-time updates, and HTML frontend.

use crate::completion_probability;
use crate::save_path_manager::{FileCategory, SavePathConfig, SavePathManager};
use crate::sla_compliance;
use crate::{DownloadManager, DownloadState, DownloadTask};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
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

/// Query parameters for export filtering (Phase 161)
#[derive(Debug, Deserialize)]
pub struct ExportFilterParams {
    #[serde(default)]
    pub states: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub group: Option<String>,
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
}

/// Request body for import operations (Phase 161)
#[derive(Debug, Deserialize)]
pub struct ImportRequest {
    /// The data to import (JSON or CSV string)
    pub data: String,
    /// Conflict strategy: "skip" (default), "overwrite", or "rename"
    pub conflict_strategy: Option<String>,
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
        .route("/api/bulk/tags", post(bulk_tags))
        .route("/api/bulk/group", post(bulk_group))
        .route("/api/bulk/priority", post(bulk_priority))
        .route("/api/bulk/speed-limit", post(bulk_speed_limit))
        .route("/api/bulk/weight", post(bulk_weight))
        .route("/api/bulk/match", post(bulk_match))
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
        .route(
            "/api/notification-center",
            get(get_notification_center_config_handler),
        )
        .route(
            "/api/notification-center",
            post(set_notification_center_config_handler),
        )
        .route(
            "/api/notification-center/summary",
            get(get_notification_center_summary_handler),
        )
        .route(
            "/api/notification-center/history",
            get(get_notification_center_history_handler),
        )
        .route(
            "/api/notification-center/history/clear",
            post(clear_notification_center_history_handler),
        )
        .route(
            "/api/notification-center/analytics",
            get(get_notification_center_analytics_handler),
        )
        .route(
            "/api/notification-center/flush",
            post(flush_notification_batch_handler),
        )
        .route(
            "/api/notification-center/event-prefs",
            post(add_event_preference_handler),
        )
        .route(
            "/api/notification-center/event-prefs/remove",
            post(remove_event_preference_handler),
        )
        .route(
            "/api/notification-preferences",
            get(get_notification_preferences_config_handler)
                .post(set_notification_preferences_config_handler),
        )
        .route(
            "/api/notification-preferences/summary",
            get(get_notification_preferences_summary_handler),
        )
        .route(
            "/api/notification-preferences/tasks",
            get(list_notification_preferences_tasks_handler),
        )
        .route(
            "/api/notification-preferences/task/:task_id",
            get(get_task_notification_preferences_handler)
                .post(set_task_notification_preferences_handler)
                .delete(remove_task_notification_preferences_handler),
        )
        .route(
            "/api/notification-preferences/task/:task_id/enable",
            post(enable_task_notifications_handler),
        )
        .route(
            "/api/notification-preferences/task/:task_id/disable",
            post(disable_task_notifications_handler),
        )
        .route(
            "/api/notification-preferences/cooldown/clear",
            post(clear_notification_cooldowns_handler),
        )
        .route(
            "/api/notification-preferences/cooldown/clear/:task_id",
            post(clear_task_notification_cooldown_handler),
        )
        .route(
            "/api/notification-preferences/check",
            post(check_notification_handler),
        )
        .route("/api/expiry", get(get_expiry_config_handler))
        .route("/api/expiry", post(set_expiry_config_handler))
        .route("/api/expiry/summary", get(get_expiry_summary_handler))
        .route("/api/expiry/refresh", post(refresh_expiries_handler))
        .route("/api/expiry/clear", post(clear_all_expiries_handler))
        .route(
            "/api/expiry/cleanup",
            post(cleanup_expired_expiries_handler),
        )
        .route("/api/expiry/report", get(get_expiry_report_handler))
        .route("/api/expiry/task/:task_id", get(get_task_expiry_handler))
        .route("/api/expiry/task/:task_id", post(set_task_expiry_handler))
        .route(
            "/api/expiry/task/:task_id/remove",
            post(remove_task_expiry_handler),
        )
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
        .route("/api/queue-completion", get(get_queue_completion_handler))
        .route(
            "/api/queue-completion",
            post(set_queue_completion_config_handler),
        )
        .route("/api/download-quota", get(get_download_quota_config))
        .route("/api/download-quota", post(set_download_quota_config))
        .route(
            "/api/download-quota/summary",
            get(get_download_quota_summary),
        )
        .route("/api/download-quota/rules", get(list_download_quota_rules))
        .route("/api/download-quota/rules", post(add_download_quota_rule))
        .route(
            "/api/download-quota/rules/:id/remove",
            post(remove_download_quota_rule),
        )
        .route(
            "/api/download-quota/rules/:id/enable",
            post(set_download_quota_rule_enabled),
        )
        .route("/api/download-quota/refresh", post(refresh_download_quota))
        .route(
            "/api/download-quota/clear",
            post(clear_download_quota_usage),
        )
        // Phase 117: Advanced Search API
        .route("/api/search", post(advanced_search_handler))
        .route("/api/search/quick/:query", get(quick_search_handler))
        .route("/api/search/stats", get(search_stats_handler))
        .route("/api/search/last", post(rerun_last_search_handler))
        // Phase 118: Automation Rules API
        .route("/api/automation", get(get_automation_summary))
        .route("/api/automation/config", get(get_automation_config))
        .route("/api/automation/config", post(set_automation_config))
        .route("/api/automation/rules", get(list_automation_rules))
        .route("/api/automation/rules", post(add_automation_rule))
        .route("/api/automation/rules/:id", get(get_automation_rule))
        .route("/api/automation/rules/:id", put(update_automation_rule))
        .route("/api/automation/rules/:id", delete(delete_automation_rule))
        .route(
            "/api/automation/rules/:id/enable",
            post(enable_automation_rule),
        )
        .route(
            "/api/automation/history/clear",
            post(clear_automation_history),
        )
        .route(
            "/api/automation/counts/reset",
            post(reset_automation_counts),
        )
        // Phase 119: Task Schedule Windows API
        .route("/api/schedule-windows", get(get_schedule_windows_summary))
        .route(
            "/api/schedule-windows/config",
            get(get_schedule_windows_config),
        )
        .route(
            "/api/schedule-windows/config",
            post(set_schedule_windows_config),
        )
        .route(
            "/api/schedule-windows/:task_id",
            get(get_task_schedule_windows),
        )
        .route(
            "/api/schedule-windows/:task_id",
            post(add_task_schedule_window),
        )
        .route(
            "/api/schedule-windows/:task_id/clear",
            post(clear_task_schedule_windows),
        )
        .route(
            "/api/schedule-windows/:task_id/:window_id",
            delete(remove_task_schedule_window),
        )
        .route(
            "/api/schedule-windows/:task_id/check",
            get(check_task_schedule_allowed),
        )
        .route("/api/auto-rules", get(list_auto_rules))
        .route("/api/auto-rules", post(add_auto_rule))
        .route("/api/auto-rules/:id/remove", post(remove_auto_rule))
        .route("/api/health", get(get_queue_health))
        .route(
            "/api/health/config",
            get(get_queue_health_config).post(set_queue_health_config),
        )
        .route(
            "/api/queue-staleness",
            get(get_queue_staleness).post(set_queue_staleness),
        )
        .route("/api/queue-staleness/check", post(check_queue_staleness))
        .route(
            "/api/queue-staleness/clear",
            post(clear_queue_staleness_promotions),
        )
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
        .route("/api/speed-prediction", get(get_speed_prediction_summary))
        .route("/api/speed-prediction", post(set_speed_prediction_config))
        .route(
            "/api/speed-prediction/predict",
            post(predict_task_speed_handler),
        )
        .route(
            "/api/speed-prediction/windows/:domain",
            get(get_optimal_speed_windows),
        )
        .route(
            "/api/speed-prediction/domain/:domain",
            get(get_domain_speed_profile),
        )
        .route(
            "/api/speed-prediction/domains",
            get(list_tracked_speed_domains),
        )
        .route(
            "/api/speed-prediction/domain/:domain/remove",
            post(remove_speed_prediction_domain),
        )
        .route(
            "/api/speed-prediction/cleanup",
            post(cleanup_old_speed_predictions),
        )
        .route(
            "/api/speed-prediction/clear",
            post(clear_all_speed_predictions),
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
        .route("/api/download-presets/:id", delete(remove_download_preset))
        .route("/api/download-presets/:id", put(update_download_preset))
        .route(
            "/api/download-presets/:id/enable",
            post(enable_download_preset),
        )
        .route(
            "/api/download-presets/:id/disable",
            post(disable_download_preset),
        )
        .route(
            "/api/download-presets/:id/apply/:task_id",
            post(apply_download_preset),
        )
        .route(
            "/api/download-presets/categories",
            get(get_preset_categories),
        )
        .route(
            "/api/download-presets/category/:category",
            get(list_presets_by_category),
        )
        .route(
            "/api/download-presets/usage-summary",
            get(get_preset_usage_summary),
        )
        .route("/api/url-bookmarks", get(list_url_bookmarks))
        .route("/api/url-bookmarks", post(add_url_bookmark))
        .route("/api/url-bookmarks/:name", get(get_url_bookmark))
        .route("/api/url-bookmarks/:name", post(remove_url_bookmark))
        .route("/api/url-bookmarks/:name/import", post(import_url_bookmark))
        .route("/api/url-bookmarks/:name/urls", post(add_urls_to_bookmark))
        .route(
            "/api/url-bookmarks/:name/urls/remove",
            post(remove_url_from_bookmark),
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
        .route("/api/download-budget", get(get_download_budget_summary))
        .route("/api/download-budget", post(set_download_budget_config))
        .route(
            "/api/download-budget/enable",
            post(set_download_budget_enabled),
        )
        .route("/api/download-budget/reset", post(reset_download_budget))
        .route(
            "/api/download-analytics",
            get(get_download_analytics_summary),
        )
        .route(
            "/api/download-analytics",
            post(set_download_analytics_config),
        )
        .route(
            "/api/download-analytics/trend",
            get(get_download_analytics_trend),
        )
        .route(
            "/api/download-analytics/today",
            get(get_download_analytics_today),
        )
        .route(
            "/api/download-analytics/records",
            get(get_download_analytics_records),
        )
        .route(
            "/api/download-analytics/prune",
            post(prune_download_analytics),
        )
        .route(
            "/api/download-analytics/clear",
            post(clear_download_analytics),
        )
        .route("/api/history-analytics", get(get_history_analytics_summary))
        .route("/api/history-analytics", post(set_history_analytics_config))
        .route(
            "/api/history-analytics/report",
            get(get_history_analytics_report),
        )
        .route(
            "/api/history-analytics/clear",
            post(clear_history_analytics),
        )
        .route("/api/url-intelligence", get(get_url_intelligence_config))
        .route("/api/url-intelligence", post(set_url_intelligence_config))
        .route("/api/url-intelligence/analyze", post(analyze_url_handler))
        .route(
            "/api/url-intelligence/cache",
            get(get_url_intelligence_cache_size),
        )
        .route(
            "/api/url-intelligence/cache",
            post(clear_url_intelligence_cache),
        )
        .route("/api/speed-benchmark", get(get_speed_benchmark_config))
        .route("/api/speed-benchmark", post(set_speed_benchmark_config))
        .route("/api/speed-benchmark/run", post(run_speed_benchmark))
        .route(
            "/api/speed-benchmark/summary",
            get(get_speed_benchmark_summary),
        )
        .route("/api/speed-benchmark/clear", post(clear_speed_benchmark))
        .route(
            "/api/speed-distribution",
            get(get_speed_distribution_config),
        )
        .route(
            "/api/speed-distribution",
            post(set_speed_distribution_config),
        )
        .route(
            "/api/speed-distribution/summary",
            get(get_speed_distribution_summary),
        )
        .route(
            "/api/speed-distribution/report",
            get(get_speed_distribution_report),
        )
        .route(
            "/api/speed-distribution/domain/:domain",
            get(get_domain_speed_stats),
        )
        .route(
            "/api/speed-distribution/protocol/:protocol",
            get(get_protocol_speed_stats),
        )
        .route(
            "/api/speed-distribution/hourly/:hour",
            get(get_hourly_speed_stats),
        )
        .route(
            "/api/speed-distribution/domains",
            get(get_tracked_speed_domains),
        )
        .route(
            "/api/speed-distribution/domain/:domain/remove",
            post(remove_speed_domain),
        )
        .route(
            "/api/speed-distribution/clear",
            post(clear_speed_distribution),
        )
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
            "/api/watch-folders/auto-scan",
            get(auto_scan_config_handler).post(set_auto_scan_config_handler),
        )
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
        .route("/api/export/csv", get(export_csv_handler))
        .route("/api/export/csv/summary", get(export_csv_summary_handler))
        // Phase 161: Task Export/Import REST API
        .route("/api/task-export", get(export_tasks_json_handler))
        .route("/api/task-export/csv", get(export_tasks_csv_handler))
        .route("/api/task-export/history", get(get_export_history_handler))
        .route("/api/task-import", post(import_tasks_handler))
        .route("/api/task-import/csv", post(import_tasks_csv_handler))
        .route("/api/task-chains", get(list_task_chains_handler))
        .route("/api/task-chains", post(create_task_chain_handler))
        .route(
            "/api/task-chains/summary",
            get(get_task_chain_summary_handler),
        )
        .route("/api/task-chains/:chain_id", get(get_task_chain_handler))
        .route(
            "/api/task-chains/:chain_id",
            delete(delete_task_chain_handler),
        )
        .route(
            "/api/task-chains/:chain_id/enable",
            post(enable_task_chain_handler),
        )
        .route(
            "/api/task-chains/:chain_id/tasks",
            post(add_task_to_chain_handler),
        )
        .route(
            "/api/task-chains/:chain_id/tasks/:task_id",
            delete(remove_task_from_chain_handler),
        )
        .route("/api/priority-aging", get(get_priority_aging_handler))
        .route("/api/priority-aging", post(set_priority_aging_handler))
        .route("/api/priority-aging/run", post(run_priority_aging_handler))
        .route("/api/task-profiler", get(get_task_profiler_handler))
        .route("/api/task-profiler", post(set_task_profiler_handler))
        .route(
            "/api/task-profiler/summary",
            get(get_performance_summary_handler),
        )
        .route(
            "/api/task-profiler/refresh",
            post(refresh_task_profiles_handler),
        )
        .route("/api/task-profiler/:task_id", get(get_task_profile_handler))
        .route(
            "/api/task-profiler/:task_id",
            delete(delete_task_profile_handler),
        )
        .route(
            "/api/task-profiler/clear",
            post(clear_task_profiles_handler),
        )
        .route(
            "/api/adaptive-concurrency",
            get(get_adaptive_concurrency_handler),
        )
        .route(
            "/api/adaptive-concurrency",
            post(set_adaptive_concurrency_handler),
        )
        .route(
            "/api/adaptive-concurrency/summary",
            get(get_adaptive_concurrency_summary_handler),
        )
        .route(
            "/api/adaptive-concurrency/evaluate",
            post(evaluate_adaptive_concurrency_handler),
        )
        .route(
            "/api/adaptive-concurrency/clear",
            post(clear_adaptive_concurrency_handler),
        )
        .route(
            "/api/download-templates",
            get(get_download_templates_handler),
        )
        .route(
            "/api/download-templates",
            post(add_download_template_handler),
        )
        .route(
            "/api/download-templates/summary",
            get(get_download_templates_summary_handler),
        )
        .route(
            "/api/download-templates/match",
            post(match_download_template_handler),
        )
        .route(
            "/api/download-templates/:id",
            get(get_download_template_handler),
        )
        .route(
            "/api/download-templates/:id",
            post(delete_download_template_handler),
        )
        .route(
            "/api/download-templates/:id/enable",
            post(enable_download_template_handler),
        )
        .route(
            "/api/download-templates/:id/disable",
            post(disable_download_template_handler),
        )
        .route(
            "/api/download-templates/:id/auto-apply",
            post(set_template_auto_apply_handler),
        )
        .route(
            "/api/download-templates/categories",
            get(get_template_categories_handler),
        )
        .route("/api/task-comments", get(get_all_task_comments_handler))
        .route(
            "/api/task-comments/search",
            post(search_task_comments_handler),
        )
        .route(
            "/api/task-comments/:task_id",
            get(get_task_comments_handler),
        )
        .route(
            "/api/task-comments/:task_id",
            post(add_task_comment_handler),
        )
        .route(
            "/api/task-comments/:task_id/config",
            get(get_task_comments_config_handler),
        )
        .route(
            "/api/task-comments/:task_id/config",
            post(set_task_comments_config_handler),
        )
        .route(
            "/api/task-comments/:task_id/:comment_id",
            axum::routing::delete(remove_task_comment_handler),
        )
        .route("/api/favorites", get(get_favorites_handler))
        .route("/api/favorites", post(add_favorite_handler))
        .route(
            "/api/favorites/:task_id/remove",
            post(remove_favorite_handler),
        )
        .route("/api/favorites/config", get(get_favorites_config_handler))
        .route("/api/favorites/config", post(set_favorites_config_handler))
        .route("/api/recycle-bin", get(list_recycled_tasks_handler))
        .route(
            "/api/recycle-bin/summary",
            get(get_recycle_bin_summary_handler),
        )
        .route(
            "/api/recycle-bin/config",
            get(get_recycle_bin_config_handler),
        )
        .route(
            "/api/recycle-bin/config",
            post(set_recycle_bin_config_handler),
        )
        .route("/api/recycle-bin/empty", post(empty_recycle_bin_handler))
        .route(
            "/api/recycle-bin/auto-purge",
            post(run_recycle_bin_auto_purge_handler),
        )
        .route(
            "/api/recycle-bin/:task_id/restore",
            post(restore_task_handler),
        )
        .route("/api/recycle-bin/:task_id/purge", post(purge_task_handler))
        .route("/api/auto-pause", get(get_auto_pause_handler))
        .route("/api/auto-pause", post(set_auto_pause_handler))
        .route("/api/auto-pause/status", get(auto_pause_status_handler))
        .route("/api/url-allowlist", get(get_allowlist_handler))
        .route("/api/url-allowlist", post(set_allowlist_handler))
        .route("/api/url-allowlist/check", post(check_allowlist_handler))
        // Phase 106: Per-task proxy override
        .route("/api/task-proxy", get(get_task_proxy_summary_handler))
        .route("/api/task-proxy/list", get(list_task_proxies_handler))
        .route("/api/task-proxy/set", post(set_task_proxy_handler))
        .route("/api/task-proxy/remove", post(remove_task_proxy_handler))
        .route(
            "/api/task-proxy/enable",
            post(set_task_proxy_enabled_handler),
        )
        .route("/api/task-proxy/notes", post(set_task_proxy_notes_handler))
        .route("/api/task-proxy/clear", post(clear_task_proxies_handler))
        .route("/api/task-snooze", get(get_snooze_handler))
        .route("/api/task-snooze", post(set_snooze_handler))
        .route("/api/task-snooze/config", get(get_snooze_config_handler))
        .route("/api/task-snooze/config", post(set_snooze_config_handler))
        .route("/api/task-scheduler", get(get_task_scheduler_handler))
        .route(
            "/api/task-scheduler",
            post(set_task_scheduler_config_handler),
        )
        .route(
            "/api/task-scheduler/config",
            get(get_task_scheduler_config_handler),
        )
        .route("/api/task-scheduler/rules", get(get_schedule_rules_handler))
        .route("/api/task-scheduler/rules", post(add_schedule_rule_handler))
        .route(
            "/api/task-scheduler/rules/:id",
            axum::routing::delete(remove_schedule_rule_handler),
        )
        .route(
            "/api/task-scheduler/rules/:id",
            post(set_schedule_rule_enabled_handler),
        )
        .route(
            "/api/task-scheduler/evaluate",
            get(evaluate_schedule_now_handler),
        )
        .route(
            "/api/progress-milestone",
            get(get_progress_milestone_handler),
        )
        .route(
            "/api/progress-milestone",
            post(set_progress_milestone_handler),
        )
        .route("/api/speed-burst", get(get_speed_burst_handler))
        .route("/api/speed-burst", post(set_speed_burst_config_handler))
        .route("/api/network-aware", get(get_network_aware_handler))
        .route("/api/network-aware", post(set_network_aware_config_handler))
        .route("/api/network-aware/status", get(get_network_status_handler))
        .route(
            "/api/network-aware/summary",
            get(get_network_aware_summary_handler),
        )
        .route(
            "/api/network-aware/probe/success",
            post(record_probe_success_handler),
        )
        .route(
            "/api/network-aware/probe/failure",
            post(record_probe_failure_handler),
        )
        .route(
            "/api/network-aware/auto-paused",
            get(get_auto_paused_tasks_handler),
        )
        .route(
            "/api/network-aware/auto-paused/clear",
            post(clear_auto_paused_handler),
        )
        .route(
            "/api/network-aware/reset",
            post(reset_network_aware_handler),
        )
        .route("/api/retry-quota", get(get_retry_quota_handler))
        .route("/api/retry-quota", post(set_retry_quota_handler))
        .route("/api/retry-quota/reset", post(reset_retry_quota_handler))
        .route("/api/ttl", get(get_ttl_handler))
        .route("/api/ttl", post(set_ttl_handler))
        .route("/api/ttl/summary", get(get_ttl_summary_handler))
        .route("/api/ttl/check", post(check_ttl_handler))
        .route("/api/error-recovery", get(get_error_recovery_handler))
        .route("/api/error-recovery", post(set_error_recovery_handler))
        .route("/api/error-recovery/classify", post(classify_error_handler))
        .route("/api/connection-health", get(get_connection_health_handler))
        .route(
            "/api/connection-health",
            post(set_connection_health_handler),
        )
        .route(
            "/api/connection-health/summary",
            get(get_connection_health_summary_handler),
        )
        .route(
            "/api/connection-health/unhealthy",
            get(get_unhealthy_connections_handler),
        )
        .route("/api/source-rotation", get(get_source_rotation_handler))
        .route("/api/source-rotation", post(set_source_rotation_handler))
        .route(
            "/api/source-rotation/summary",
            get(get_source_rotation_summary_handler),
        )
        .route(
            "/api/source-rotation/execute",
            post(execute_source_rotation_handler),
        )
        .route(
            "/api/bandwidth-allocation",
            get(get_bandwidth_allocation_handler),
        )
        .route(
            "/api/bandwidth-allocation",
            post(set_bandwidth_allocation_handler),
        )
        .route(
            "/api/bandwidth-allocation/plan",
            get(get_bandwidth_allocation_plan_handler),
        )
        .route("/api/speed-burst/start", post(start_speed_burst_handler))
        .route("/api/speed-burst/stop", post(stop_speed_burst_handler))
        .route("/api/snapshots", get(list_snapshots_handler))
        .route("/api/snapshots", post(create_snapshot_handler))
        .route("/api/snapshots/:id", get(get_snapshot_handler))
        .route("/api/snapshots/:id/restore", post(restore_snapshot_handler))
        .route(
            "/api/snapshots/:id",
            axum::routing::delete(delete_snapshot_handler),
        )
        .route("/api/network-monitor", get(get_network_monitor_handler))
        .route("/api/network-monitor", post(set_network_monitor_handler))
        .route(
            "/api/network-monitor/clear",
            post(clear_network_monitor_handler),
        )
        .route(
            "/api/download-time-limit",
            get(get_download_time_limit_handler),
        )
        .route(
            "/api/download-time-limit",
            post(set_download_time_limit_handler),
        )
        .route("/api/auto-actions", get(get_auto_actions_handler))
        .route("/api/auto-actions", post(set_auto_actions_handler))
        .route(
            "/api/auto-actions/summary",
            get(get_auto_actions_summary_handler),
        )
        .route(
            "/api/auto-actions/rules",
            get(list_auto_action_rules_handler),
        )
        .route(
            "/api/auto-actions/rules",
            post(add_auto_action_rule_handler),
        )
        .route(
            "/api/auto-actions/rules/:id",
            axum::routing::delete(remove_auto_action_rule_handler),
        )
        .route(
            "/api/auto-actions/rules/:id/enable",
            post(set_auto_action_rule_enabled_handler),
        )
        .route(
            "/api/auto-actions/task/:task_id",
            post(set_task_auto_action_handler),
        )
        .route(
            "/api/auto-actions/task/:task_id",
            axum::routing::delete(remove_task_auto_action_handler),
        )
        .route(
            "/api/auto-actions/history/clear",
            post(clear_auto_actions_history_handler),
        )
        .route("/api/deadline", get(get_deadline_handler))
        .route("/api/deadline", post(set_deadline_handler))
        .route("/api/deadline/summary", get(get_deadline_summary_handler))
        .route("/api/deadline/refresh", post(refresh_deadlines_handler))
        .route("/api/deadline/clear", post(clear_deadlines_handler))
        .route(
            "/api/deadline/task/:task_id",
            post(set_task_deadline_handler),
        )
        .route(
            "/api/deadline/task/:task_id",
            axum::routing::delete(remove_task_deadline_handler),
        )
        .route("/api/integrity", get(get_integrity_handler))
        .route("/api/integrity", post(set_integrity_handler))
        .route("/api/integrity/summary", get(get_integrity_summary_handler))
        .route("/api/integrity/verify", post(verify_integrity_handler))
        .route(
            "/api/integrity/verify/:task_id",
            post(verify_task_integrity_handler),
        )
        .route("/api/integrity/clear", post(clear_integrity_handler))
        .route("/api/disk-monitor", get(get_disk_monitor_handler))
        .route("/api/disk-monitor", post(set_disk_monitor_handler))
        .route("/api/disk-monitor/check", post(check_disk_space_handler))
        .route("/api/disk-monitor/start", post(start_disk_monitor_handler))
        .route("/api/disk-monitor/stop", post(stop_disk_monitor_handler))
        .route("/api/global-budget", get(get_global_budget_handler))
        .route("/api/global-budget", post(set_global_budget_handler))
        .route(
            "/api/global-budget/summary",
            get(global_budget_summary_handler),
        )
        .route(
            "/api/global-budget/reset",
            post(reset_global_budget_handler),
        )
        .route(
            "/api/global-budget/resume",
            post(resume_global_budget_handler),
        )
        .route("/api/source-benchmark", get(get_source_benchmark_handler))
        .route("/api/source-benchmark", post(set_source_benchmark_handler))
        .route(
            "/api/source-benchmark/run",
            post(run_source_benchmark_handler),
        )
        .route(
            "/api/source-benchmark/select",
            post(select_best_source_handler),
        )
        .route(
            "/api/source-benchmark/cache",
            get(get_source_benchmark_cache_handler),
        )
        .route(
            "/api/source-benchmark/cache",
            post(clear_source_benchmark_cache_handler),
        )
        .route("/api/backup", get(list_backups_handler))
        .route("/api/backup", post(create_backup_handler))
        .route("/api/backup/:id", get(get_backup_handler))
        .route("/api/backup/:id", delete(delete_backup_handler))
        .route("/api/preflight", get(get_preflight_handler))
        .route("/api/preflight", post(set_preflight_handler))
        .route("/api/preflight/run", post(run_preflight_handler))
        .route("/api/cost", get(get_cost_config_handler))
        .route("/api/cost", post(set_cost_config_handler))
        .route("/api/cost/summary", get(get_cost_summary_handler))
        .route("/api/cost/summary/month", get(get_cost_monthly_handler))
        .route("/api/cost/summary/all", get(get_cost_all_handler))
        .route("/api/cost/tasks", get(get_cost_tasks_handler))
        .route("/api/cost/daily", get(get_cost_daily_handler))
        .route("/api/cost/clear", post(clear_cost_handler))
        .route("/api/speed-test", get(get_speed_test_config_handler))
        .route("/api/speed-test", post(set_speed_test_config_handler))
        .route("/api/speed-test/run", post(run_speed_test_handler))
        .route(
            "/api/speed-test/summary",
            get(get_speed_test_summary_handler),
        )
        .route(
            "/api/speed-test/history",
            get(get_speed_test_history_handler),
        )
        .route("/api/speed-test/latest", get(get_speed_test_latest_handler))
        .route("/api/speed-test/clear", post(clear_speed_test_handler))
        .route("/api/speed-trend", get(get_speed_trend_config_handler))
        .route("/api/speed-trend", post(set_speed_trend_config_handler))
        .route(
            "/api/speed-trend/summary",
            get(get_speed_trend_summary_handler),
        )
        .route("/api/speed-trend/trends", get(get_all_speed_trends_handler))
        .route(
            "/api/speed-trend/degrading",
            get(get_degrading_trends_handler),
        )
        .route(
            "/api/speed-trend/improving",
            get(get_improving_trends_handler),
        )
        .route(
            "/api/speed-trend/clear",
            post(clear_all_speed_trends_handler),
        )
        .route(
            "/api/task-scorecard",
            get(get_task_scorecard_config_handler),
        )
        .route(
            "/api/task-scorecard",
            post(set_task_scorecard_config_handler),
        )
        .route(
            "/api/task-scorecard/summary",
            get(get_task_scorecard_summary_handler),
        )
        .route(
            "/api/task-scorecard/list",
            get(list_task_scorecards_handler),
        )
        .route(
            "/api/task-scorecard/top",
            get(get_top_task_scorecards_handler),
        )
        .route(
            "/api/task-scorecard/worst",
            get(get_worst_task_scorecards_handler),
        )
        .route(
            "/api/task-scorecard/generate/:task_id",
            post(generate_task_scorecard_handler),
        )
        .route(
            "/api/task-scorecard/:task_id",
            get(get_task_scorecard_handler),
        )
        .route(
            "/api/task-scorecard/:task_id",
            delete(delete_task_scorecard_handler),
        )
        .route(
            "/api/task-scorecard/clear",
            post(clear_all_task_scorecards_handler),
        )
        .route("/api/webhook", get(get_webhook_summary_handler))
        .route("/api/webhook/config", get(get_webhook_config_handler))
        .route("/api/webhook/config", post(set_webhook_config_handler))
        .route(
            "/api/webhook/endpoints",
            get(list_webhook_endpoints_handler),
        )
        .route("/api/webhook/endpoints", post(add_webhook_endpoint_handler))
        .route(
            "/api/webhook/endpoints/:id",
            get(get_webhook_endpoint_handler),
        )
        .route(
            "/api/webhook/endpoints/:id",
            put(update_webhook_endpoint_handler),
        )
        .route(
            "/api/webhook/endpoints/:id",
            delete(remove_webhook_endpoint_handler),
        )
        .route(
            "/api/webhook/endpoints/:id/history",
            get(get_webhook_history_handler),
        )
        .route(
            "/api/webhook/endpoints/:id/history",
            post(clear_webhook_history_handler),
        )
        .route(
            "/api/webhook/history",
            post(clear_all_webhook_history_handler),
        )
        .route(
            "/api/path-organizer",
            get(get_path_organizer_config_handler),
        )
        .route(
            "/api/path-organizer",
            post(set_path_organizer_config_handler),
        )
        .route(
            "/api/path-organizer/summary",
            get(get_path_organizer_summary_handler),
        )
        .route(
            "/api/path-organizer/reset",
            post(reset_path_organizer_summary_handler),
        )
        .route(
            "/api/path-organizer/categories",
            get(list_file_categories_handler),
        )
        .route(
            "/api/path-organizer/categories",
            post(add_file_category_handler),
        )
        .route(
            "/api/path-organizer/categories/:name",
            delete(remove_file_category_handler),
        )
        .route(
            "/api/path-organizer/organize/:task_id",
            post(organize_task_handler),
        )
        // Upload Tracker API
        .route(
            "/api/upload-tracker",
            get(get_upload_tracker_config_handler),
        )
        .route(
            "/api/upload-tracker",
            post(set_upload_tracker_config_handler),
        )
        .route(
            "/api/upload-tracker/summary",
            get(get_upload_tracker_summary_handler),
        )
        .route(
            "/api/upload-tracker/clear",
            post(clear_upload_tracker_handler),
        )
        .route(
            "/api/upload-tracker/tasks",
            get(list_upload_tracked_tasks_handler),
        )
        // Data Retention API
        .route(
            "/api/data-retention",
            get(get_data_retention_config_handler),
        )
        .route(
            "/api/data-retention",
            post(set_data_retention_config_handler),
        )
        .route(
            "/api/data-retention/summary",
            get(get_data_retention_summary_handler),
        )
        .route(
            "/api/data-retention/rules",
            get(list_data_retention_rules_handler),
        )
        .route(
            "/api/data-retention/rules",
            post(add_data_retention_rule_handler),
        )
        .route(
            "/api/data-retention/rules/:id",
            delete(remove_data_retention_rule_handler),
        )
        .route(
            "/api/data-retention/cleanup",
            post(execute_data_retention_cleanup_handler),
        )
        .route(
            "/api/data-retention/history",
            get(get_data_retention_history_handler),
        )
        .route(
            "/api/data-retention/history/clear",
            post(clear_data_retention_history_handler),
        )
        // Source Quality API
        .route(
            "/api/source-quality",
            get(get_source_quality_config_handler),
        )
        .route(
            "/api/source-quality",
            post(set_source_quality_config_handler),
        )
        .route(
            "/api/source-quality/summary",
            get(get_source_quality_summary_handler),
        )
        .route(
            "/api/source-quality/:source_id",
            get(get_source_quality_detail_handler),
        )
        .route(
            "/api/source-quality/:source_id/unblock",
            post(unblock_source_quality_handler),
        )
        .route(
            "/api/source-quality/:source_id",
            delete(remove_source_quality_handler),
        )
        .route(
            "/api/source-quality/recommend",
            post(recommend_source_quality_handler),
        )
        .route(
            "/api/source-quality/clear",
            post(clear_source_quality_handler),
        )
        // Phase 138: Speed Boost CLI + REST API Integration
        .route("/api/speed-boost", get(get_speed_boost_status_handler))
        .route("/api/speed-boost", post(set_speed_boost_config_handler))
        .route("/api/speed-boost/start", post(start_speed_boost_handler))
        .route("/api/speed-boost/stop", post(stop_speed_boost_handler))
        .route(
            "/api/speed-boost/preset",
            post(add_speed_boost_preset_handler),
        )
        .route(
            "/api/speed-boost/preset/:id",
            axum::routing::delete(remove_speed_boost_preset_handler),
        )
        .route(
            "/api/speed-boost/presets",
            get(list_speed_boost_presets_handler),
        )
        .route(
            "/api/speed-boost/scheduled",
            get(list_speed_boost_scheduled_handler),
        )
        .route(
            "/api/speed-boost/scheduled",
            post(add_speed_boost_scheduled_handler),
        )
        .route(
            "/api/speed-boost/scheduled/:id",
            axum::routing::delete(remove_speed_boost_scheduled_handler),
        )
        // Phase 136: Bandwidth Forecast CLI + REST API Integration
        .route(
            "/api/bandwidth-forecast",
            get(get_bandwidth_forecast_config_handler),
        )
        .route(
            "/api/bandwidth-forecast",
            post(set_bandwidth_forecast_config_handler),
        )
        .route(
            "/api/bandwidth-forecast/summary",
            get(get_bandwidth_forecast_summary_handler),
        )
        .route(
            "/api/bandwidth-forecast/predict/:domain",
            get(predict_bandwidth_handler),
        )
        .route(
            "/api/bandwidth-forecast/domain/:domain",
            axum::routing::delete(remove_bandwidth_forecast_domain_handler),
        )
        .route(
            "/api/bandwidth-forecast/clear",
            post(clear_bandwidth_forecast_handler),
        )
        // Phase 140: Intelligent Source Selector API
        .route(
            "/api/intelligent-selector",
            get(get_intelligent_selector_config_handler),
        )
        .route(
            "/api/intelligent-selector",
            post(set_intelligent_selector_config_handler),
        )
        .route(
            "/api/intelligent-selector/summary",
            get(get_intelligent_selector_summary_handler),
        )
        .route(
            "/api/intelligent-selector/select/:task_id",
            post(select_intelligent_sources_handler),
        )
        .route(
            "/api/intelligent-selector/candidates/:task_id",
            get(get_intelligent_selector_candidates_handler),
        )
        .route(
            "/api/intelligent-selector/history",
            get(get_intelligent_selector_history_handler),
        )
        .route(
            "/api/intelligent-selector/history/clear",
            post(clear_intelligent_selector_history_handler),
        )
        .route(
            "/api/intelligent-selector/clear",
            post(clear_intelligent_selector_handler),
        )
        // Phase 159: Progress Prediction API
        .route(
            "/api/progress-prediction",
            get(get_progress_prediction_config_handler)
                .post(set_progress_prediction_config_handler),
        )
        .route(
            "/api/progress-prediction/predict/:task_id",
            get(predict_task_completion_handler),
        )
        .route(
            "/api/progress-prediction/predict-all",
            get(predict_all_tasks_handler),
        )
        .route(
            "/api/progress-prediction/accuracy",
            get(get_prediction_accuracy_handler),
        )
        .route(
            "/api/progress-prediction/task/:task_id",
            axum::routing::delete(remove_prediction_task_handler),
        )
        .route(
            "/api/progress-prediction/clear",
            post(clear_prediction_data_handler),
        )
        // Phase 161: Link Rot Detection API
        .route(
            "/api/link-rot",
            get(get_link_rot_config_handler).post(set_link_rot_config_handler),
        )
        .route("/api/link-rot/summary", get(get_link_rot_summary_handler))
        .route("/api/link-rot/report", get(get_link_rot_report_handler))
        .route("/api/link-rot/batch", get(get_link_rot_batch_handler))
        .route("/api/link-rot/clear", post(clear_link_rot_handler))
        .route("/api/link-rot/save", post(save_link_rot_handler))
        .route("/api/link-rot/:task_id", get(get_link_rot_task_handler))
        // Phase 142: Retry Budget API
        .route(
            "/api/retry-budget",
            get(get_retry_budget_config_handler).post(set_retry_budget_config_handler),
        )
        .route(
            "/api/retry-budget/summary",
            get(get_retry_budget_summary_handler),
        )
        .route(
            "/api/retry-budget/check/:domain",
            get(check_retry_budget_handler),
        )
        .route(
            "/api/retry-budget/record-retry/:domain",
            post(record_retry_budget_retry_handler),
        )
        .route(
            "/api/retry-budget/record-success/:domain",
            post(record_retry_budget_success_handler),
        )
        .route(
            "/api/retry-budget/clear/:domain",
            post(clear_domain_retry_budget_handler),
        )
        .route(
            "/api/retry-budget/clear",
            post(clear_all_retry_budget_handler),
        )
        .route("/api/uptime", get(get_uptime_handler))
        // ── Save Path Manager API (Phase 162) ──
        .route("/api/save-path", get(get_save_path_config_handler))
        .route("/api/save-path", post(set_save_path_config_handler))
        .route("/api/save-path/validate", get(validate_save_path_handler))
        .route(
            "/api/save-path/predict/:filename",
            get(predict_save_path_handler),
        )
        .route(
            "/api/save-path/category-dirs",
            get(get_category_dirs_handler),
        )
        .route(
            "/api/save-path/category-dirs",
            post(set_category_dir_handler),
        )
        .route(
            "/api/save-path/category-dirs/:category",
            axum::routing::delete(remove_category_dir_handler),
        )
        // ── Dependency Visualization API (Phase 154) ──
        .route("/api/dependency-visualization", get(get_dep_viz_handler))
        .route(
            "/api/dependency-visualization/stats",
            get(get_dep_viz_stats_handler),
        )
        .route(
            "/api/dependency-visualization/config",
            get(get_dep_viz_config_handler).post(set_dep_viz_config_handler),
        )
        .route(
            "/api/dependency-visualization/cycles",
            get(get_dep_viz_cycles_handler),
        )
        .route(
            "/api/dependency-visualization/roots",
            get(get_dep_viz_roots_handler),
        )
        .route(
            "/api/dependency-visualization/leaves",
            get(get_dep_viz_leaves_handler),
        )
        .route(
            "/api/dependency-visualization/text",
            get(get_dep_viz_text_handler),
        )
        .route(
            "/api/dependency-visualization/dot",
            get(get_dep_viz_dot_handler),
        )
        // ── Download Diagnostics API (Phase 156) ──
        .route("/api/diagnostics", get(get_diagnostics_handler))
        .route(
            "/api/diagnostics/config",
            get(get_diagnostics_config_handler),
        )
        .route(
            "/api/diagnostics/config",
            post(set_diagnostics_config_handler),
        )
        .route("/api/diagnostics/run", post(run_diagnostics_handler))
        .route(
            "/api/diagnostics/report",
            get(get_diagnostics_report_handler),
        )
        // Phase 158: Speed Heatmap REST API
        .route(
            "/api/speed-heatmap",
            get(get_speed_heatmap_config_handler)
                .post(set_speed_heatmap_config_handler)
                .delete(reset_speed_heatmap_handler),
        )
        .route(
            "/api/speed-heatmap/summary",
            get(get_speed_heatmap_summary_handler),
        )
        .route(
            "/api/speed-heatmap/report",
            get(get_speed_heatmap_report_handler),
        )
        .route(
            "/api/speed-heatmap/hourly/:hour",
            get(get_speed_heatmap_hourly_handler),
        )
        .route(
            "/api/speed-heatmap/daily/:day",
            get(get_speed_heatmap_daily_handler),
        )
        .route(
            "/api/speed-heatmap/quality/:day/:hour",
            get(get_speed_heatmap_quality_handler),
        )
        .route(
            "/api/speed-heatmap/prune",
            post(prune_speed_heatmap_handler),
        )
        .route(
            "/api/file-stats",
            get(get_file_stats_handler).post(set_file_stats_handler),
        )
        .route(
            "/api/file-stats/summary",
            get(get_file_stats_summary_handler),
        )
        .route("/api/file-stats/clear", post(clear_file_stats_handler))
        .route(
            "/api/file-stats/extension/:ext",
            get(get_extension_stats_handler),
        )
        // Phase 153: URL Blacklist REST API
        .route(
            "/api/url-blacklist",
            get(get_url_blacklist_config_handler).post(set_url_blacklist_config_handler),
        )
        .route(
            "/api/url-blacklist/enable",
            post(set_url_blacklist_enabled_handler),
        )
        .route(
            "/api/url-blacklist/entries",
            get(list_blacklist_entries_handler).post(add_blacklist_entry_handler),
        )
        .route(
            "/api/url-blacklist/entries/:id",
            delete(remove_blacklist_entry_handler),
        )
        .route("/api/url-blacklist/check", post(check_url_blocked_handler))
        // Phase 153: Download Cooldown REST API
        .route(
            "/api/cooldown",
            get(get_cooldown_config_handler).post(set_cooldown_config_handler),
        )
        .route("/api/cooldown/status", post(get_cooldown_status_handler))
        .route("/api/cooldown/tick", post(tick_cooldown_handler))
        .route("/api/cooldown/reset/:task_id", post(reset_cooldown_handler))
        .route("/api/cooldown/summary", get(get_cooldown_summary_handler))
        // Phase 153: URL Health Monitor REST API
        .route(
            "/api/url-health",
            get(get_url_health_config_handler).post(set_url_health_config_handler),
        )
        .route(
            "/api/url-health/summary",
            get(get_url_health_summary_handler),
        )
        .route("/api/url-health/checks", get(get_url_health_checks_handler))
        .route("/api/url-health/monitor", post(monitor_url_health_handler))
        .route(
            "/api/url-health/monitor/:url",
            delete(unmonitor_url_health_handler),
        )
        .route(
            "/api/url-health/check/:url",
            get(get_url_health_check_handler),
        )
        .route("/api/url-health/cleanup", post(cleanup_dead_urls_handler))
        // Phase 148: Task Activity REST API
        .route(
            "/api/task-activity",
            get(get_all_activity_summaries_handler),
        )
        .route(
            "/api/task-activity/:task_id",
            get(get_task_activity_handler).delete(clear_task_activity_handler),
        )
        .route(
            "/api/task-activity/:task_id/log",
            post(log_task_activity_handler),
        )
        // Phase 144: SLA Compliance REST API
        .route(
            "/api/sla-compliance",
            get(get_sla_config_handler).post(set_sla_config_handler),
        )
        .route("/api/sla-compliance/summary", get(get_sla_summary_handler))
        .route(
            "/api/sla-compliance/definitions",
            get(list_sla_definitions_handler).post(add_sla_definition_handler),
        )
        .route(
            "/api/sla-compliance/definitions/:id",
            get(get_sla_definition_handler).delete(delete_sla_definition_handler),
        )
        .route(
            "/api/sla-compliance/definitions/:id/enable",
            post(set_sla_enabled_handler),
        )
        .route(
            "/api/sla-compliance/evaluate",
            post(evaluate_sla_compliance_handler),
        )
        .route(
            "/api/sla-compliance/history/:id",
            get(get_sla_history_handler),
        )
        .route(
            "/api/sla-compliance/history/:id/clear",
            post(clear_sla_history_handler),
        )
        .route(
            "/api/sla-compliance/history/clear",
            post(clear_all_sla_history_handler),
        )
        .route("/api/sla-compliance/report", get(get_sla_report_handler))
        // Phase 167: Speed Alert REST API
        .route(
            "/api/speed-alerts",
            get(get_speed_alert_config_handler).post(set_speed_alert_config_handler),
        )
        .route(
            "/api/speed-alerts/summary",
            get(get_speed_alert_summary_handler),
        )
        .route(
            "/api/speed-alerts/history",
            get(get_speed_alert_history_handler),
        )
        .route(
            "/api/speed-alerts/history/clear",
            post(clear_speed_alert_history_handler),
        )
        .route(
            "/api/speed-alerts/task/:task_id",
            get(get_task_speed_alerts_handler),
        )
        .route(
            "/api/speed-alerts/task/:task_id/remove",
            post(remove_speed_alert_task_handler),
        )
        .route(
            "/api/speed-alerts/enable",
            post(set_speed_alert_enabled_handler),
        )
        .route(
            "/api/speed-alerts/monitors/clear",
            post(clear_speed_alert_monitors_handler),
        )
        // Phase 165: Download Session REST API
        .route(
            "/api/download-session",
            get(get_all_session_summaries_handler),
        )
        .route(
            "/api/download-session/config",
            get(get_download_session_config_handler).post(set_download_session_config_handler),
        )
        .route(
            "/api/download-session/summary",
            get(get_download_session_summary_handler),
        )
        .route(
            "/api/download-session/task/:task_id",
            get(get_task_session_summary_handler).delete(remove_task_sessions_handler),
        )
        .route(
            "/api/download-session/clear",
            post(clear_all_sessions_handler),
        )
        // Phase 164: Connection Pool REST API
        .route(
            "/api/connection-pool",
            get(get_connection_pool_status_handler).post(set_connection_pool_config_handler),
        )
        .route(
            "/api/connection-pool/stats",
            get(get_connection_pool_stats_handler),
        )
        .route(
            "/api/connection-pool/config",
            get(get_connection_pool_config_handler),
        )
        .route(
            "/api/connection-pool/domains",
            get(get_connection_pool_domains_handler),
        )
        .route(
            "/api/connection-pool/domain/:domain",
            post(set_connection_pool_domain_limit_handler),
        )
        .route(
            "/api/connection-pool/cleanup",
            post(cleanup_connection_pool_handler),
        )
        .route(
            "/api/connection-pool/clear",
            post(clear_connection_pool_handler),
        )
        .route(
            "/api/connection-pool/save",
            post(save_connection_pool_config_handler),
        )
        .route(
            "/api/source-reliability",
            get(get_source_reliability_config_handler).post(set_source_reliability_config_handler),
        )
        .route(
            "/api/source-reliability/summary",
            get(get_source_reliability_summary_handler),
        )
        .route(
            "/api/source-reliability/report",
            get(get_source_reliability_report_handler),
        )
        .route(
            "/api/source-reliability/domains",
            get(list_source_reliability_domains_handler),
        )
        .route(
            "/api/source-reliability/domain/:domain",
            get(get_source_reliability_domain_handler)
                .delete(clear_source_reliability_domain_handler),
        )
        .route(
            "/api/source-reliability/score/:domain",
            get(get_source_reliability_score_handler),
        )
        .route(
            "/api/source-reliability/avoid",
            get(get_source_reliability_avoid_handler),
        )
        .route(
            "/api/source-reliability/prune",
            post(prune_source_reliability_handler),
        )
        .route(
            "/api/source-reliability/clear",
            post(clear_source_reliability_handler),
        )
        .route(
            "/api/host-conn-limit",
            get(get_host_conn_limit_config_handler).post(set_host_conn_limit_config_handler),
        )
        .route(
            "/api/host-conn-limit/summary",
            get(get_host_conn_limit_summary_handler),
        )
        .route(
            "/api/host-conn-limit/host/:hostname",
            get(get_host_conn_state_handler),
        )
        .route(
            "/api/host-conn-limit/host/:hostname/acquire",
            post(acquire_host_connection_handler),
        )
        .route(
            "/api/host-conn-limit/host/:hostname/release",
            post(release_host_connection_handler),
        )
        .route(
            "/api/host-conn-limit/host/:hostname/failure",
            post(record_host_failure_handler),
        )
        .route(
            "/api/host-conn-limit/host/:hostname/remove",
            post(remove_host_connection_handler),
        )
        .route(
            "/api/host-conn-limit/overrides",
            get(list_host_overrides_handler).post(set_host_override_handler),
        )
        .route(
            "/api/host-conn-limit/overrides/:hostname",
            delete(remove_host_override_handler),
        )
        .route(
            "/api/host-conn-limit/clear",
            post(clear_host_connections_handler),
        )
        .route(
            "/api/host-conn-limit/cleanup",
            post(cleanup_stale_hosts_handler),
        )
        .route(
            "/api/task-cron-scheduler",
            get(get_task_cron_scheduler_config_handler)
                .post(set_task_cron_scheduler_config_handler),
        )
        .route(
            "/api/task-cron-scheduler/summary",
            get(get_task_cron_scheduler_summary_handler),
        )
        .route(
            "/api/task-cron-scheduler/schedules",
            get(list_task_cron_schedules_handler),
        )
        .route(
            "/api/task-cron-scheduler/schedules/:task_id",
            post(add_task_cron_schedule_handler).delete(remove_task_cron_schedule_handler),
        )
        .route(
            "/api/task-cron-scheduler/schedules/:task_id/enable",
            post(set_task_cron_schedule_enabled_handler),
        )
        .route(
            "/api/source-latency",
            get(get_source_latency_config_handler).post(set_source_latency_config_handler),
        )
        .route(
            "/api/source-latency/summary",
            get(get_source_latency_summary_handler),
        )
        .route(
            "/api/source-latency/domain/:domain",
            get(get_source_latency_domain_handler),
        )
        .route(
            "/api/source-latency/all",
            get(get_source_latency_all_handler),
        )
        .route(
            "/api/source-latency/best",
            get(get_best_latency_domain_handler),
        )
        .route(
            "/api/source-latency/rank",
            get(rank_domains_by_latency_handler),
        )
        .route(
            "/api/source-latency/clear/:domain",
            post(clear_source_latency_domain_handler),
        )
        .route(
            "/api/source-latency/clear",
            post(clear_source_latency_all_handler),
        )
        .route(
            "/api/source-latency/decay",
            post(apply_source_latency_decay_handler),
        )
        // ── Phase 151: Bandwidth QoS REST API ─────────────────────────────
        .route(
            "/api/bandwidth-qos",
            get(get_bandwidth_qos_config_handler).post(set_bandwidth_qos_config_handler),
        )
        .route(
            "/api/bandwidth-qos/summary",
            get(get_bandwidth_qos_summary_handler),
        )
        .route(
            "/api/bandwidth-qos/assign/:task_id",
            post(assign_qos_tier_handler),
        )
        .route(
            "/api/bandwidth-qos/assign/:task_id/remove",
            post(remove_qos_assignment_handler),
        )
        .route(
            "/api/bandwidth-qos/task/:task_id",
            get(get_task_qos_tier_handler),
        )
        .route(
            "/api/bandwidth-qos/rules",
            get(list_qos_rules_handler).post(add_qos_rule_handler),
        )
        .route(
            "/api/bandwidth-qos/rules/:rule_id",
            delete(remove_qos_rule_handler),
        )
        .route(
            "/api/bandwidth-qos/rules/:rule_id/enable",
            post(set_qos_rule_enabled_handler),
        )
        .route(
            "/api/bandwidth-qos/rules/:rule_id/priority",
            post(set_qos_rule_priority_handler),
        )
        .route(
            "/api/bandwidth-qos/clear/assignments",
            post(clear_qos_assignments_handler),
        )
        .route(
            "/api/bandwidth-qos/clear/rules",
            post(clear_qos_rules_handler),
        )
        // Phase 149: Bandwidth Usage Tracker REST API
        .route(
            "/api/bandwidth-usage",
            get(get_bandwidth_usage_config_handler).post(set_bandwidth_usage_config_handler),
        )
        .route(
            "/api/bandwidth-usage/summary",
            get(get_bandwidth_usage_summary_handler),
        )
        .route(
            "/api/bandwidth-usage/24h",
            get(get_bandwidth_usage_24h_handler),
        )
        .route(
            "/api/bandwidth-usage/peak-hours",
            get(get_bandwidth_usage_peak_hours_handler),
        )
        .route(
            "/api/bandwidth-usage/clear",
            post(clear_bandwidth_usage_handler),
        )
        .route(
            "/api/bandwidth-usage/format",
            get(get_bandwidth_usage_format_handler),
        )
        .route(
            "/api/completion-probability",
            get(get_completion_probability_config_handler)
                .post(set_completion_probability_config_handler),
        )
        .route(
            "/api/completion-probability/summary",
            get(get_completion_probability_summary_handler),
        )
        .route(
            "/api/completion-probability/estimate",
            post(estimate_completion_probability_handler),
        )
        .route(
            "/api/completion-probability/cache/:task_id",
            get(get_cached_completion_probability_handler),
        )
        .route(
            "/api/completion-probability/cache",
            post(clear_completion_probability_cache_handler),
        )
        // Phase 167: Dynamic Priority REST API
        .route(
            "/api/dynamic-priority",
            get(get_dynamic_priority_config_handler).post(set_dynamic_priority_config_handler),
        )
        .route(
            "/api/dynamic-priority/summary",
            get(get_dynamic_priority_summary_handler),
        )
        .route(
            "/api/dynamic-priority/enable",
            post(set_dynamic_priority_enabled_handler),
        )
        .route(
            "/api/dynamic-priority/run",
            post(run_dynamic_priority_handler),
        )
        .route(
            "/api/dynamic-priority/clear",
            post(clear_dynamic_priority_handler),
        )
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

/// Get download budget summary (weekly/monthly)
async fn get_download_budget_summary(
    State(state): State<Arc<WebState>>,
) -> Json<crate::download_budget::BudgetSummary> {
    let summary = state.manager.get_download_budget_summary().await;
    Json(summary)
}

/// Set download budget configuration
#[derive(Deserialize)]
struct SetDownloadBudgetRequest {
    weekly_limit_bytes: Option<u64>,
    monthly_limit_bytes: Option<u64>,
    auto_pause: Option<bool>,
}

async fn set_download_budget_config(
    State(state): State<Arc<WebState>>,
    Json(req): Json<SetDownloadBudgetRequest>,
) -> Json<serde_json::Value> {
    let current = state.manager.get_download_budget_config().await;
    let config = crate::download_budget::BudgetConfig {
        enabled: true,
        weekly_limit_bytes: req.weekly_limit_bytes.unwrap_or(current.weekly_limit_bytes),
        monthly_limit_bytes: req
            .monthly_limit_bytes
            .unwrap_or(current.monthly_limit_bytes),
        auto_pause: req.auto_pause.unwrap_or(current.auto_pause),
        ..current
    };
    state.manager.set_download_budget_config(config).await;
    Json(serde_json::json!({"success": true}))
}

/// Enable or disable download budget
#[derive(Deserialize)]
struct DownloadBudgetEnabledRequest {
    enabled: bool,
}

async fn set_download_budget_enabled(
    State(state): State<Arc<WebState>>,
    Json(req): Json<DownloadBudgetEnabledRequest>,
) -> Json<serde_json::Value> {
    state.manager.set_download_budget_enabled(req.enabled).await;
    Json(serde_json::json!({"success": true}))
}

/// Reset download budget usage
async fn reset_download_budget(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.reset_download_budget().await;
    Json(serde_json::json!({"success": true}))
}

/// Get download analytics summary (last N days)
#[derive(Deserialize)]
struct AnalyticsSummaryQuery {
    #[serde(default = "default_analytics_days")]
    days: u32,
}

fn default_analytics_days() -> u32 {
    7
}

async fn get_download_analytics_summary(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(query): axum::extract::Query<AnalyticsSummaryQuery>,
) -> Json<serde_json::Value> {
    match state
        .manager
        .get_download_analytics_summary(query.days)
        .await
    {
        Some(summary) => Json(serde_json::to_value(summary).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "No analytics data for the requested period"})),
    }
}

/// Set download analytics configuration
async fn set_download_analytics_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_analytics::AnalyticsConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_download_analytics_config(config).await;
    Json(serde_json::json!({"success": true}))
}

/// Get analytics trend comparison
#[derive(Deserialize)]
struct AnalyticsTrendQuery {
    #[serde(default = "default_analytics_days")]
    days: u32,
}

async fn get_download_analytics_trend(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(query): axum::extract::Query<AnalyticsTrendQuery>,
) -> Json<serde_json::Value> {
    match state.manager.get_download_analytics_trend(query.days).await {
        Some(trend) => Json(serde_json::to_value(trend).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "Insufficient data for trend comparison"})),
    }
}

/// Get today's analytics metrics
async fn get_download_analytics_today(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    match state.manager.get_download_analytics_today().await {
        Some(today) => Json(serde_json::to_value(today).unwrap_or_default()),
        None => Json(serde_json::json!({"message": "No analytics data recorded today"})),
    }
}

/// Get all analytics records
async fn get_download_analytics_records(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::download_analytics::DailyMetrics>> {
    let records = state.manager.get_download_analytics_records().await;
    Json(records)
}

/// Prune old analytics records
async fn prune_download_analytics(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.prune_download_analytics().await;
    Json(serde_json::json!({"success": true}))
}

/// Clear all analytics data
async fn clear_download_analytics(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_download_analytics().await;
    Json(serde_json::json!({"success": true}))
}

/// Get history analytics summary
#[derive(Deserialize)]
struct HistoryAnalyticsQuery {
    #[serde(default = "default_history_analytics_days")]
    days: i64,
}

fn default_history_analytics_days() -> i64 {
    30
}

async fn get_history_analytics_summary(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(query): axum::extract::Query<HistoryAnalyticsQuery>,
) -> Json<serde_json::Value> {
    let summary = state
        .manager
        .get_history_analytics_for_period(query.days)
        .await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// Set history analytics configuration
async fn set_history_analytics_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_history_analytics::HistoryAnalyticsConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_history_analytics_config(config).await;
    Json(serde_json::json!({"success": true}))
}

/// Get formatted history analytics report
async fn get_history_analytics_report(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(query): axum::extract::Query<HistoryAnalyticsQuery>,
) -> Json<serde_json::Value> {
    let summary = state
        .manager
        .get_history_analytics_for_period(query.days)
        .await;
    let report = state.manager.format_history_analytics(&summary).await;
    Json(serde_json::json!({"report": report}))
}

/// Clear history analytics data
async fn clear_history_analytics(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_history_analytics().await;
    Json(serde_json::json!({"success": true}))
}

/// Get speed benchmark configuration
async fn get_speed_benchmark_config(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_benchmark::BenchmarkConfig> {
    let config = state.manager.get_speed_benchmark_config().await;
    Json(config)
}

/// Set speed benchmark configuration
async fn set_speed_benchmark_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_benchmark::BenchmarkConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_speed_benchmark_config(config).await;
    Json(serde_json::json!({"success": true}))
}

/// Run speed benchmark on URLs
async fn run_speed_benchmark(
    State(state): State<Arc<WebState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let urls: Vec<String> = request
        .get("urls")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    if urls.is_empty() {
        return Json(serde_json::json!({"error": "No URLs provided"}));
    }
    let summary = state.manager.benchmark_urls(&urls).await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// Get speed benchmark summary
async fn get_speed_benchmark_summary(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_benchmark::BenchmarkSummary> {
    let summary = state.manager.get_benchmark_summary().await;
    Json(summary)
}

/// Clear speed benchmark results
async fn clear_speed_benchmark(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_benchmarks().await;
    Json(serde_json::json!({"success": true}))
}

// ========== Speed Distribution API Handlers ==========

/// GET /api/speed-distribution - Get speed distribution configuration
async fn get_speed_distribution_config(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_distribution::SpeedDistributionConfig> {
    let config = state.manager.get_speed_distribution_config().await;
    Json(config)
}

/// POST /api/speed-distribution - Update speed distribution configuration
async fn set_speed_distribution_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_distribution::SpeedDistributionConfig>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .set_speed_distribution_config(config)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"success": true})))
}

/// GET /api/speed-distribution/summary - Get speed distribution summary
async fn get_speed_distribution_summary(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_distribution::SpeedDistributionSummary> {
    let summary = state.manager.get_speed_distribution_summary().await;
    Json(summary)
}

/// GET /api/speed-distribution/report - Get formatted speed distribution report
async fn get_speed_distribution_report(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let report = state.manager.format_speed_distribution_report().await;
    Json(serde_json::json!({"report": report}))
}

/// GET /api/speed-distribution/domain/:domain - Get speed stats for a specific domain
async fn get_domain_speed_stats(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Result<Json<crate::speed_distribution::SpeedStats>, StatusCode> {
    state
        .manager
        .get_domain_speed_stats(&domain)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// GET /api/speed-distribution/protocol/:protocol - Get speed stats for a specific protocol
async fn get_protocol_speed_stats(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(protocol): axum::extract::Path<String>,
) -> Result<Json<crate::speed_distribution::SpeedStats>, StatusCode> {
    let proto = crate::speed_distribution::SpeedProtocol::from_str(&protocol);
    state
        .manager
        .get_protocol_speed_stats(proto)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// GET /api/speed-distribution/hourly/:hour - Get speed stats for a specific hour
async fn get_hourly_speed_stats(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hour): axum::extract::Path<u8>,
) -> Result<Json<crate::speed_distribution::SpeedStats>, StatusCode> {
    if hour > 23 {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .manager
        .get_hourly_speed_stats(hour)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// GET /api/speed-distribution/domains - Get list of tracked domains
async fn get_tracked_speed_domains(State(state): State<Arc<WebState>>) -> Json<Vec<String>> {
    let domains = state.manager.get_tracked_speed_domains().await;
    Json(domains)
}

/// POST /api/speed-distribution/domain/:domain/remove - Remove a domain from tracking
async fn remove_speed_domain(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let removed = state.manager.remove_speed_domain(&domain).await;
    Json(serde_json::json!({"removed": removed}))
}

/// POST /api/speed-distribution/clear - Clear all speed distribution data
async fn clear_speed_distribution(
    State(state): State<Arc<WebState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .clear_speed_distribution()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"success": true})))
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

/// GET /api/watch-folders/auto-scan - Get auto-scan configuration
async fn auto_scan_config_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_watch_folder_auto_scan().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "interval_secs": config.interval_secs,
        "last_auto_scan": config.last_auto_scan
    }))
}

/// POST /api/watch-folders/auto-scan - Set auto-scan configuration
#[derive(serde::Deserialize)]
struct SetAutoScanConfigRequest {
    enabled: Option<bool>,
    interval_secs: Option<u64>,
}

async fn set_auto_scan_config_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<SetAutoScanConfigRequest>,
) -> Json<serde_json::Value> {
    let current = state.manager.get_watch_folder_auto_scan().await;
    let enabled = req.enabled.unwrap_or(current.enabled);
    let interval_secs = req.interval_secs.unwrap_or(current.interval_secs);

    let success = state
        .manager
        .set_watch_folder_auto_scan(enabled, interval_secs)
        .await;

    Json(serde_json::json!({
        "success": success,
        "enabled": enabled,
        "interval_secs": interval_secs,
        "message": if success { "Auto-scan configuration updated" } else { "Failed to update configuration" }
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
    let config = state.manager.get_queue_health_config().await;
    let report = state.manager.get_queue_health_report(&config).await;
    Json(report)
}

/// Get queue health monitor configuration
async fn get_queue_health_config(
    State(state): State<Arc<WebState>>,
) -> Json<crate::queue_health::HealthMonitorConfig> {
    Json(state.manager.get_queue_health_config().await)
}

/// Set queue health monitor configuration
async fn set_queue_health_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::queue_health::HealthMonitorConfig>,
) -> impl IntoResponse {
    match state.manager.set_queue_health_config(config).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Get queue staleness configuration and current summary
async fn get_queue_staleness(
    State(state): State<Arc<WebState>>,
) -> Json<crate::queue_staleness::StalenessSummary> {
    Json(state.manager.check_queue_staleness().await)
}

/// Update queue staleness configuration
async fn set_queue_staleness(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::queue_staleness::StalenessConfig>,
) -> StatusCode {
    state.manager.set_queue_staleness_config(config).await;
    StatusCode::OK
}

/// Check queue for stale tasks and optionally promote them
async fn check_queue_staleness(
    State(state): State<Arc<WebState>>,
) -> Json<crate::queue_staleness::StalenessSummary> {
    Json(state.manager.check_queue_staleness().await)
}

/// Clear all staleness promotion counts for all tasks
async fn clear_queue_staleness_promotions(State(state): State<Arc<WebState>>) -> StatusCode {
    state.manager.clear_queue_staleness_promotions().await;
    StatusCode::OK
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

/// Export tasks to CSV format
async fn export_csv_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    match state.manager.export_tasks_to_csv_string(None).await {
        Ok(csv) => (
            StatusCode::OK,
            [("Content-Type", "text/csv; charset=utf-8")],
            csv,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

/// Get CSV export summary statistics
async fn export_csv_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_csv_summary().await;
    (
        StatusCode::OK,
        [("Content-Type", "text/plain; charset=utf-8")],
        summary,
    )
}

// ===== Phase 161: Task Export/Import Handlers =====

/// Export tasks to JSON format
async fn export_tasks_json_handler(
    State(state): State<Arc<WebState>>,
    Query(params): Query<ExportFilterParams>,
) -> impl axum::response::IntoResponse {
    let filter = crate::task_export::ExportFilter {
        states: params.states.clone(),
        tags: params.tags.clone(),
        group: params.group.clone(),
        created_after: params.created_after,
        created_before: params.created_before,
    };

    match state.manager.export_tasks_json(filter).await {
        Ok(tasks) => Json(serde_json::json!({
            "status": "ok",
            "count": tasks.len(),
            "tasks": tasks
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

/// Export tasks to CSV format
async fn export_tasks_csv_handler(
    State(state): State<Arc<WebState>>,
    Query(params): Query<ExportFilterParams>,
) -> impl axum::response::IntoResponse {
    let filter = crate::task_export::ExportFilter {
        states: params.states.clone(),
        tags: params.tags.clone(),
        group: params.group.clone(),
        created_after: params.created_after,
        created_before: params.created_before,
    };

    match state.manager.export_tasks_csv(filter).await {
        Ok(csv) => (
            StatusCode::OK,
            [("Content-Type", "text/csv; charset=utf-8")],
            csv,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

/// Get export history
async fn get_export_history_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let history = state.manager.get_export_history().await;
    Json(serde_json::json!({
        "history": history,
        "count": history.len()
    }))
}

/// Import tasks from JSON data
async fn import_tasks_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<ImportRequest>,
) -> impl axum::response::IntoResponse {
    let conflict_strategy = match req.conflict_strategy.as_deref() {
        Some("overwrite") => crate::task_export::ImportConflictStrategy::Overwrite,
        Some("rename") => crate::task_export::ImportConflictStrategy::Rename,
        _ => crate::task_export::ImportConflictStrategy::Skip,
    };

    match state
        .manager
        .import_tasks_json(&req.data, conflict_strategy)
        .await
    {
        Ok(result) => Json(serde_json::json!({
            "status": "ok",
            "result": result
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

/// Import tasks from CSV data
async fn import_tasks_csv_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<ImportRequest>,
) -> impl axum::response::IntoResponse {
    let conflict_strategy = match req.conflict_strategy.as_deref() {
        Some("overwrite") => crate::task_export::ImportConflictStrategy::Overwrite,
        Some("rename") => crate::task_export::ImportConflictStrategy::Rename,
        _ => crate::task_export::ImportConflictStrategy::Skip,
    };

    match state
        .manager
        .import_tasks_csv(&req.data, conflict_strategy)
        .await
    {
        Ok(result) => Json(serde_json::json!({
            "status": "ok",
            "result": result
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

// ===== Task Chain Handlers =====

/// GET /api/task-chains - List all task chains
async fn list_task_chains_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::task_chain::TaskChain>> {
    let chains = state.manager.list_task_chains().await;
    Json(chains)
}

/// POST /api/task-chains - Create a new task chain
async fn create_task_chain_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let chain_id = req.get("chain_id").and_then(|v| v.as_str()).unwrap_or("");
    let name = req.get("name").and_then(|v| v.as_str()).unwrap_or("");

    if chain_id.is_empty() || name.is_empty() {
        return Json(serde_json::json!({"error": "chain_id and name are required"}));
    }

    match state
        .manager
        .create_task_chain(chain_id.to_string(), name.to_string())
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok", "chain_id": chain_id})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/task-chains/summary - Get task chain summary
async fn get_task_chain_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::task_chain::TaskChainSummary> {
    let summary = state.manager.get_task_chain_summary().await;
    Json(summary)
}

/// GET /api/task-chains/:chain_id - Get a specific task chain
async fn get_task_chain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(chain_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_task_chain(&chain_id).await {
        Some(chain) => Json(serde_json::json!({"chain": chain})),
        None => Json(serde_json::json!({"error": "Chain not found"})),
    }
}

/// DELETE /api/task-chains/:chain_id - Delete a task chain
async fn delete_task_chain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(chain_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.delete_task_chain(&chain_id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/task-chains/:chain_id/enable - Enable or disable a task chain
async fn enable_task_chain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(chain_id): axum::extract::Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let enabled = req.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);

    match state
        .manager
        .set_task_chain_enabled(&chain_id, enabled)
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok", "enabled": enabled})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/task-chains/:chain_id/tasks - Add a task to a chain
async fn add_task_to_chain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(chain_id): axum::extract::Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = req.get("task_id").and_then(|v| v.as_str()).unwrap_or("");

    if task_id.is_empty() {
        return Json(serde_json::json!({"error": "task_id is required"}));
    }

    match state
        .manager
        .add_task_to_chain(&chain_id, task_id.to_string())
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok", "task_id": task_id})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// DELETE /api/task-chains/:chain_id/tasks/:task_id - Remove a task from a chain
async fn remove_task_from_chain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path((chain_id, task_id)): axum::extract::Path<(String, String)>,
) -> impl axum::response::IntoResponse {
    match state
        .manager
        .remove_task_from_chain(&chain_id, &task_id)
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/priority-aging - Get priority aging configuration
async fn get_priority_aging_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::priority_aging::PriorityAgingConfig> {
    let config = state.manager.get_priority_aging_config().await;
    Json(config)
}

/// POST /api/priority-aging - Set priority aging configuration
async fn set_priority_aging_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::priority_aging::PriorityAgingConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_priority_aging_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/priority-aging/run - Run priority aging check
async fn run_priority_aging_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::priority_aging::AgingDecision>> {
    let decisions = state.manager.run_priority_aging().await;
    Json(decisions)
}

/// GET /api/task-profiler - Get task profiler configuration
async fn get_task_profiler_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::task_profiler::TaskProfilerConfig> {
    let config = state.manager.get_task_profiler_config().await;
    Json(config)
}

/// POST /api/task-profiler - Set task profiler configuration
async fn set_task_profiler_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_profiler::TaskProfilerConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_task_profiler_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/task-profiler/summary - Get performance summary
async fn get_performance_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::task_profiler::PerformanceSummary> {
    let summary = state.manager.get_performance_summary(5).await;
    Json(summary)
}

/// POST /api/task-profiler/refresh - Refresh all task profiles
async fn refresh_task_profiles_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.refresh_task_profiles().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/task-profiler/:task_id - Get profile for a specific task
async fn get_task_profile_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_task_profile(&task_id).await {
        Some(profile) => Json(serde_json::json!({"profile": profile})),
        None => Json(serde_json::json!({"error": "Profile not found"})),
    }
}

/// DELETE /api/task-profiler/:task_id - Remove a task profile
async fn delete_task_profile_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let removed = state.manager.remove_task_profile(&task_id).await;
    Json(serde_json::json!({"removed": removed}))
}

/// POST /api/task-profiler/clear - Clear all task profiles
async fn clear_task_profiles_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_task_profiles().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/adaptive-concurrency - Get adaptive concurrency configuration
async fn get_adaptive_concurrency_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::adaptive_concurrency::AdaptiveConcurrencyConfig> {
    let config = state.manager.get_adaptive_concurrency_config().await;
    Json(config)
}

/// POST /api/adaptive-concurrency - Set adaptive concurrency configuration
async fn set_adaptive_concurrency_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::adaptive_concurrency::AdaptiveConcurrencyConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_adaptive_concurrency_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/adaptive-concurrency/summary - Get adaptive concurrency summary
async fn get_adaptive_concurrency_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::adaptive_concurrency::AdaptiveConcurrencySummary> {
    let summary = state.manager.get_adaptive_concurrency_summary().await;
    Json(summary)
}

/// POST /api/adaptive-concurrency/evaluate - Evaluate and adjust concurrency
async fn evaluate_adaptive_concurrency_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let decisions = state.manager.evaluate_adaptive_concurrency().await;
    let adjusted = decisions.len();
    Json(
        serde_json::json!({"adjusted_tasks": adjusted, "decisions": decisions.iter().map(|(id, d)| {
        serde_json::json!({"task_id": id, "decision": format!("{:?}", d)})
    }).collect::<Vec<_>>()}),
    )
}

/// POST /api/adaptive-concurrency/clear - Clear all adaptive concurrency state
async fn clear_adaptive_concurrency_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_adaptive_concurrency().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ─── Download Templates (Phase 100) ────────────────────────────────

/// GET /api/download-templates - List all download templates
async fn get_download_templates_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::download_templates::DownloadTemplate>> {
    let templates = state.manager.list_download_templates().await;
    Json(templates)
}

/// POST /api/download-templates - Add or update a download template
async fn add_download_template_handler(
    State(state): State<Arc<WebState>>,
    Json(template): Json<crate::download_templates::DownloadTemplate>,
) -> Json<serde_json::Value> {
    state.manager.add_download_template(template).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/download-templates/summary - Get template summaries
async fn get_download_templates_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summaries = state.manager.list_download_template_summaries().await;
    let stats = state.manager.get_template_stats().await;
    Json(serde_json::json!({
        "summaries": summaries,
        "stats": stats
    }))
}

/// POST /api/download-templates/match - Find templates matching a URL
async fn match_download_template_handler(
    State(state): State<Arc<WebState>>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    if let Some(url) = payload.get("url").and_then(|v| v.as_str()) {
        let templates = state.manager.find_matching_templates(url).await;
        Json(serde_json::json!({"matching_templates": templates}))
    } else {
        Json(serde_json::json!({"error": "Missing 'url' field"}))
    }
}

/// GET /api/download-templates/:id - Get a specific template
async fn get_download_template_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    if let Some(template) = state.manager.get_download_template(&id).await {
        Json(serde_json::json!({"template": template}))
    } else {
        Json(serde_json::json!({"error": "Template not found"}))
    }
}

/// POST /api/download-templates/:id - Delete a template
async fn delete_download_template_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    if state.manager.remove_download_template(&id).await.is_some() {
        Json(serde_json::json!({"status": "deleted"}))
    } else {
        Json(serde_json::json!({"error": "Template not found"}))
    }
}

/// POST /api/download-templates/:id/enable - Enable a template
async fn enable_download_template_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    if state.manager.set_template_enabled(&id, true).await {
        Json(serde_json::json!({"status": "enabled"}))
    } else {
        Json(serde_json::json!({"error": "Template not found"}))
    }
}

/// POST /api/download-templates/:id/disable - Disable a template
async fn disable_download_template_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    if state.manager.set_template_enabled(&id, false).await {
        Json(serde_json::json!({"status": "disabled"}))
    } else {
        Json(serde_json::json!({"error": "Template not found"}))
    }
}

/// POST /api/download-templates/:id/auto-apply - Set auto-apply for a template
async fn set_template_auto_apply_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    if let Some(auto_apply) = payload.get("auto_apply").and_then(|v| v.as_bool()) {
        if state.manager.set_template_auto_apply(&id, auto_apply).await {
            Json(serde_json::json!({"status": "ok", "auto_apply": auto_apply}))
        } else {
            Json(serde_json::json!({"error": "Template not found"}))
        }
    } else {
        Json(serde_json::json!({"error": "Missing 'auto_apply' field"}))
    }
}

/// GET /api/download-templates/categories - List all template categories
async fn get_template_categories_handler(State(state): State<Arc<WebState>>) -> Json<Vec<String>> {
    let categories = state.manager.list_template_categories().await;
    Json(categories)
}

/// GET /api/task-comments - List all tasks with comments and counts
async fn get_all_task_comments_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let task_ids = state.manager.list_tasks_with_comments().await;
    let counts = state.manager.get_task_comment_counts().await;
    let config = state.manager.get_task_comments_config().await;
    let total: usize = counts.values().sum();

    Json(serde_json::json!({
        "tasks": task_ids,
        "counts": counts,
        "total_comments": total,
        "config": config,
    }))
}

/// POST /api/task-comments/search - Search comments across all tasks
async fn search_task_comments_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let query = body.get("query").and_then(|v| v.as_str()).unwrap_or("");

    if query.is_empty() {
        return Json(serde_json::json!({"error": "query is required"}));
    }

    let result = state.manager.search_task_comments(query).await;
    Json(serde_json::json!({
        "query": result.query,
        "total_matches": result.total_matches,
        "matches": result.matches,
    }))
}

/// GET /api/task-comments/:task_id - Get comments for a specific task
async fn get_task_comments_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_task_comment_summary(&task_id).await;
    Json(summary)
}

/// POST /api/task-comments/:task_id - Add a comment to a task
async fn add_task_comment_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let text = body.get("text").and_then(|v| v.as_str()).unwrap_or("");

    if text.trim().is_empty() {
        return Json(serde_json::json!({"error": "text is required and cannot be empty"}));
    }

    let author = body
        .get("author")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tags: Vec<String> = body
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    match state
        .manager
        .add_task_comment(&task_id, text, author.as_deref(), tags)
        .await
    {
        Ok(comment) => Json(serde_json::json!({"status": "ok", "comment": comment})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/task-comments/:task_id/config - Get task comments configuration
async fn get_task_comments_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::task_comments::TaskCommentsConfig> {
    let config = state.manager.get_task_comments_config().await;
    Json(config)
}

/// POST /api/task-comments/:task_id/config - Set task comments configuration
async fn set_task_comments_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_comments::TaskCommentsConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_task_comments_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// DELETE /api/task-comments/:task_id/:comment_id - Remove a comment
async fn remove_task_comment_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path((_task_id, comment_id)): axum::extract::Path<(String, String)>,
) -> impl axum::response::IntoResponse {
    match state.manager.remove_task_comment(&comment_id).await {
        Ok(removed) => Json(serde_json::json!({"status": "ok", "removed": removed})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Get task favorites list
async fn get_favorites_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let ids = state.manager.get_favorite_ids().await;
    let count = state.manager.get_favorites_count().await;
    Json(serde_json::json!({
        "favorite_ids": ids,
        "count": count
    }))
}

/// Add task to favorites
async fn add_favorite_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = body.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
    let note = body
        .get("note")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if task_id.is_empty() {
        return Json(serde_json::json!({"error": "task_id is required"}));
    }

    match state.manager.add_favorite(task_id, note).await {
        Ok(()) => Json(serde_json::json!({"status": "ok", "task_id": task_id})),
        Err(e) => Json(serde_json::json!({"error": e})),
    }
}

/// Remove task from favorites
async fn remove_favorite_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_favorite(&task_id).await;
    Json(serde_json::json!({"status": "ok", "removed": removed}))
}

/// Get favorites configuration
async fn get_favorites_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_favorites_config().await;
    Json(serde_json::json!({
        "max_favorites": config.max_favorites
    }))
}

/// Set favorites configuration
async fn set_favorites_config_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let max_favorites = body
        .get("max_favorites")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let config = crate::task_favorites::FavoritesConfig { max_favorites };
    state.manager.set_favorites_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// List all recycled tasks in the recycle bin
async fn list_recycled_tasks_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let entries = state.manager.list_recycled_tasks().await;
    Json(entries)
}

/// Get recycle bin summary statistics
async fn get_recycle_bin_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_recycle_bin_summary().await;
    Json(serde_json::json!({
        "total_entries": summary.total_entries,
        "total_size": summary.total_size,
        "total_downloaded": summary.total_downloaded,
        "oldest_entry": summary.oldest_entry,
        "newest_entry": summary.newest_entry,
        "by_protocol": summary.by_protocol,
        "config_enabled": summary.config_enabled,
        "auto_purge_after_secs": summary.auto_purge_after_secs,
        "max_entries": summary.max_entries
    }))
}

/// Get recycle bin configuration
async fn get_recycle_bin_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_recycle_bin_config().await;
    Json(config)
}

/// Set recycle bin configuration
async fn set_recycle_bin_config_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let mut config = state.manager.get_recycle_bin_config().await;

    if let Some(enabled) = body.get("enabled").and_then(|v| v.as_bool()) {
        config.enabled = enabled;
    }
    if let Some(secs) = body.get("auto_purge_after_secs").and_then(|v| v.as_u64()) {
        config.auto_purge_after_secs = secs;
    }
    if let Some(max) = body.get("max_entries").and_then(|v| v.as_u64()) {
        config.max_entries = max as usize;
    }

    match state.manager.set_recycle_bin_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Empty the entire recycle bin
async fn empty_recycle_bin_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let count = state.manager.empty_recycle_bin().await;
    Json(serde_json::json!({"purged": count}))
}

/// Run auto-purge on the recycle bin
async fn run_recycle_bin_auto_purge_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let purged = state.manager.run_recycle_bin_auto_purge().await;
    Json(serde_json::json!({"purged": purged}))
}

/// Restore a task from the recycle bin
async fn restore_task_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.restore_task(&task_id).await {
        Some(id) => Json(serde_json::json!({"status": "ok", "task_id": id})),
        None => Json(serde_json::json!({"error": "Task not found in recycle bin"})),
    }
}

/// Permanently delete a task from the recycle bin
async fn purge_task_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    if state.manager.purge_task(&task_id).await {
        Json(serde_json::json!({"status": "ok"}))
    } else {
        Json(serde_json::json!({"error": "Task not found in recycle bin"}))
    }
}

/// Get auto-pause configuration
async fn get_auto_pause_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::auto_pause::AutoPauseConfig> {
    let config = state.manager.get_auto_pause_config().await;
    Json(config)
}

/// Set auto-pause configuration
async fn set_auto_pause_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::auto_pause::AutoPauseConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_auto_pause_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Get auto-pause status
async fn auto_pause_status_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::auto_pause::AutoPauseStatus> {
    let status = state.manager.get_auto_pause_status().await;
    Json(status)
}

/// Get URL allowlist configuration
async fn get_allowlist_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::url_allowlist::AllowlistConfig> {
    let config = state.manager.get_url_allowlist_config().await;
    Json(config)
}

/// Set URL allowlist configuration
async fn set_allowlist_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::url_allowlist::AllowlistConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_url_allowlist_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Check if a URL is allowed by the allowlist
async fn check_allowlist_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let url = body.get("url").and_then(|v| v.as_str()).unwrap_or("");
    if url.is_empty() {
        return Json(serde_json::json!({"error": "missing url field"}));
    }
    let result = state.manager.check_url_allowed(url).await;
    Json(serde_json::to_value(result).unwrap_or_default())
}

// ─── Phase 106: Per-Task Proxy Override Handlers ────────────────────────

/// Get summary of per-task proxy overrides
async fn get_task_proxy_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_task_proxy_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// List all per-task proxy overrides
async fn list_task_proxies_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let proxies = state.manager.list_task_proxies().await;
    Json(serde_json::json!({
        "proxies": proxies,
        "count": proxies.len()
    }))
}

/// Set a per-task proxy override
async fn set_task_proxy_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = match body.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => return Json(serde_json::json!({"error": "missing or empty task_id field"})),
    };
    let proxy_url = match body.get("proxy_url").and_then(|v| v.as_str()) {
        Some(url) if !url.is_empty() => url.to_string(),
        _ => return Json(serde_json::json!({"error": "missing or empty proxy_url field"})),
    };
    let notes = body
        .get("notes")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Parse proxy URL (supports socks5://host:port and http://host:port)
    let proxy_config = match parse_proxy_url_for_task(&proxy_url) {
        Ok(config) => config,
        Err(e) => return Json(serde_json::json!({"error": e})),
    };

    match state
        .manager
        .set_task_proxy(task_id, proxy_config, notes)
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Remove a per-task proxy override
async fn remove_task_proxy_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = match body.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id,
        _ => return Json(serde_json::json!({"error": "missing or empty task_id field"})),
    };

    match state.manager.remove_task_proxy(task_id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Enable or disable a per-task proxy override
async fn set_task_proxy_enabled_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = match body.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id,
        _ => return Json(serde_json::json!({"error": "missing or empty task_id field"})),
    };
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    match state.manager.set_task_proxy_enabled(task_id, enabled).await {
        Ok(()) => Json(serde_json::json!({"status": "ok", "enabled": enabled})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Update notes for a per-task proxy override
async fn set_task_proxy_notes_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let task_id = match body.get("task_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id,
        _ => return Json(serde_json::json!({"error": "missing or empty task_id field"})),
    };
    let notes = body
        .get("notes")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    match state.manager.set_task_proxy_notes(task_id, notes).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Clear all per-task proxy overrides
async fn clear_task_proxies_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    match state.manager.clear_task_proxies().await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Parse a proxy URL string into a ProxyConfig for task proxy
fn parse_proxy_url_for_task(url: &str) -> Result<crate::proxy::ProxyConfig, String> {
    use crate::proxy::{ProxyConfig, ProxyType};

    if let Some(rest) = url.strip_prefix("socks5://") {
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("invalid socks5 URL format, expected socks5://host:port".to_string());
        }
        let port: u16 = parts[1]
            .parse()
            .map_err(|_| "invalid port number".to_string())?;
        Ok(ProxyConfig::new(
            ProxyType::Socks5,
            parts[0].to_string(),
            port,
        ))
    } else if let Some(rest) = url.strip_prefix("http://") {
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("invalid http proxy URL format, expected http://host:port".to_string());
        }
        let port: u16 = parts[1]
            .parse()
            .map_err(|_| "invalid port number".to_string())?;
        Ok(ProxyConfig::new(
            ProxyType::Http,
            parts[0].to_string(),
            port,
        ))
    } else {
        Err("proxy URL must start with socks5:// or http://".to_string())
    }
}

/// Get task snooze status (list of snoozed tasks)
async fn get_snooze_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let snoozed = state.manager.list_snoozed_tasks().await;
    Json(serde_json::json!({
        "snoozed_tasks": snoozed,
        "count": snoozed.len()
    }))
}

/// Snooze or unsnooze a task
async fn set_snooze_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let action = body.get("action").and_then(|v| v.as_str()).unwrap_or("");
    match action {
        "snooze" => {
            let task_id = body.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            let until_str = body.get("until").and_then(|v| v.as_str()).unwrap_or("");
            let reason = body
                .get("reason")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if task_id.is_empty() || until_str.is_empty() {
                return Json(serde_json::json!({"error": "missing task_id or until field"}));
            }
            let until = match chrono::DateTime::parse_from_rfc3339(until_str) {
                Ok(dt) => dt.with_timezone(&chrono::Utc),
                Err(_) => {
                    return Json(
                        serde_json::json!({"error": "invalid until format (use RFC3339)"}),
                    );
                }
            };
            match state.manager.snooze_task(task_id, until, reason).await {
                Ok(state) => Json(serde_json::json!({"status": "ok", "snooze": state})),
                Err(e) => Json(serde_json::json!({"error": e.to_string()})),
            }
        }
        "unsnooze" => {
            let task_id = body.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            if task_id.is_empty() {
                return Json(serde_json::json!({"error": "missing task_id field"}));
            }
            match state.manager.unsnooze_task(task_id).await {
                Ok(_) => Json(serde_json::json!({"status": "ok"})),
                Err(e) => Json(serde_json::json!({"error": e.to_string()})),
            }
        }
        _ => Json(serde_json::json!({"error": "action must be 'snooze' or 'unsnooze'"})),
    }
}

/// Get task snooze configuration
async fn get_snooze_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_task_snooze_config().await;
    Json(config)
}

/// Set task snooze configuration
async fn set_snooze_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_snooze::TaskSnoozeConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_task_snooze_config(config).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

// ─── Phase 115: Task Scheduler API ───

/// Get task scheduler evaluation (current schedule state)
async fn get_task_scheduler_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let evaluation = state.manager.evaluate_schedule_now().await;
    let rules = state.manager.get_schedule_rules().await;
    let config = state.manager.get_task_scheduler_config().await;
    Json(serde_json::json!({
        "evaluation": evaluation,
        "rules": rules,
        "config": config
    }))
}

/// Set task scheduler configuration
async fn set_task_scheduler_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_scheduler::TaskSchedulerConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_task_scheduler_config(config).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Add a schedule rule
async fn add_schedule_rule_handler(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::task_scheduler::ScheduleRule>,
) -> impl axum::response::IntoResponse {
    match state.manager.add_schedule_rule(rule).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Remove a schedule rule
async fn remove_schedule_rule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.remove_schedule_rule(&rule_id).await {
        Ok(true) => Json(serde_json::json!({"status": "ok"})),
        Ok(false) => Json(serde_json::json!({"error": "rule not found"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Enable/disable a schedule rule
async fn set_schedule_rule_enabled_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match state
        .manager
        .set_schedule_rule_enabled(&rule_id, enabled)
        .await
    {
        Ok(true) => Json(serde_json::json!({"status": "ok"})),
        Ok(false) => Json(serde_json::json!({"error": "rule not found"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Get task scheduler configuration
async fn get_task_scheduler_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_task_scheduler_config().await;
    Json(config)
}

/// Get all schedule rules
async fn get_schedule_rules_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let rules = state.manager.get_schedule_rules().await;
    Json(rules)
}

/// Evaluate schedules at the current time
async fn evaluate_schedule_now_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let evaluation = state.manager.evaluate_schedule_now().await;
    Json(evaluation)
}

/// Get progress milestone configuration
async fn get_progress_milestone_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_progress_milestone_config().await;
    Json(config)
}

/// Set progress milestone configuration
async fn set_progress_milestone_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::progress_milestone::ProgressMilestoneConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_progress_milestone_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get speed burst status and configuration
async fn get_speed_burst_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let status = state.manager.get_speed_burst_status().await;
    let config = state.manager.get_speed_burst_config().await;
    Json(serde_json::json!({
        "config": config,
        "status": {
            "active_bursts": status.active_bursts,
            "total_bursts_started": status.total_bursts_started,
            "total_bursts_completed": status.total_bursts_completed
        }
    }))
}

/// Set speed burst configuration
async fn set_speed_burst_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_burst::SpeedBurstConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_speed_burst_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get network-aware configuration and current status
async fn get_network_aware_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_network_aware_config().await;
    let status = state.manager.get_network_status().await;
    Json(serde_json::json!({
        "config": config,
        "status": status.to_string()
    }))
}

/// Set network-aware configuration
async fn set_network_aware_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::network_aware::NetworkAwareConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_network_aware_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get current network status
async fn get_network_status_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let status = state.manager.get_network_status().await;
    Json(serde_json::json!({"status": status.to_string()}))
}

/// Get network-aware summary
async fn get_network_aware_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_network_aware_summary().await;
    Json(summary)
}

/// Record a successful connectivity probe
async fn record_probe_success_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let transitioned = state.manager.record_network_probe_success().await;
    let status = state.manager.get_network_status().await;
    Json(serde_json::json!({
        "transitioned": transitioned,
        "status": status.to_string()
    }))
}

/// Record a failed connectivity probe
async fn record_probe_failure_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let transitioned = state.manager.record_network_probe_failure().await;
    let status = state.manager.get_network_status().await;
    Json(serde_json::json!({
        "transitioned": transitioned,
        "status": status.to_string()
    }))
}

/// Get list of auto-paused task IDs
async fn get_auto_paused_tasks_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let task_ids = state.manager.get_network_auto_paused_tasks().await;
    Json(serde_json::json!({"auto_paused_tasks": task_ids}))
}

/// Clear auto-paused task tracking
async fn clear_auto_paused_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_network_auto_paused().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Reset network-aware state
async fn reset_network_aware_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.reset_network_aware().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get deadline configuration
async fn get_deadline_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_deadline_config().await;
    Json(serde_json::json!({
        "config": config,
        "enabled": config.enabled,
        "low_threshold_hours": config.low_threshold_hours,
        "medium_threshold_hours": config.medium_threshold_hours,
        "high_threshold_hours": config.high_threshold_hours
    }))
}

/// Set deadline configuration
async fn set_deadline_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_deadline::DeadlineConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_deadline_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get deadline summary
async fn get_deadline_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_deadline_summary().await;
    Json(summary)
}

/// Refresh all deadline urgency levels
async fn refresh_deadlines_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.refresh_deadlines().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Clear all deadlines
async fn clear_deadlines_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_all_deadlines().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Set deadline for a specific task
async fn set_task_deadline_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let deadline_str = body.get("deadline").and_then(|v| v.as_str()).unwrap_or("");
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    match chrono::DateTime::parse_from_rfc3339(deadline_str) {
        Ok(dt) => {
            state
                .manager
                .set_task_deadline(&task_id, dt.with_timezone(&chrono::Utc), enabled)
                .await;
            Json(serde_json::json!({"status": "ok", "task_id": task_id}))
        }
        Err(e) => Json(serde_json::json!({"error": format!("Invalid deadline format: {}", e)})),
    }
}

/// Remove deadline for a specific task
async fn remove_task_deadline_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_task_deadline(&task_id).await;
    Json(serde_json::json!({"removed": removed, "task_id": task_id}))
}

/// Get integrity verification configuration
async fn get_integrity_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_integrity_config().await;
    Json(serde_json::json!({
        "config": config,
        "auto_verify_on_complete": config.auto_verify_on_complete,
        "periodic_verification": config.periodic_verification,
        "verification_interval_secs": config.verification_interval_secs
    }))
}

/// Set integrity verification configuration
async fn set_integrity_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::integrity_verification::IntegrityConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_integrity_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get integrity verification summary
async fn get_integrity_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_integrity_summary().await;
    Json(summary)
}

/// Verify all completed tasks' integrity
async fn verify_integrity_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let results = state.manager.verify_all_integrity().await;
    let summary = state.manager.get_integrity_summary().await;
    Json(serde_json::json!({
        "results": results,
        "summary": summary
    }))
}

/// Verify a specific task's integrity
async fn verify_task_integrity_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.verify_task_integrity(&task_id).await {
        Some(result) => Json(serde_json::json!({
            "found": true,
            "result": result
        })),
        None => Json(serde_json::json!({
            "found": false,
            "error": "Task not found"
        })),
    }
}

/// Clear all integrity verification results
async fn clear_integrity_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_integrity_results().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get disk monitor config and summary
async fn get_disk_monitor_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_disk_monitor_config().await;
    let summary = state.manager.get_disk_monitor_summary().await;
    Json(serde_json::json!({
        "config": config,
        "summary": summary
    }))
}

/// Set disk monitor configuration
async fn set_disk_monitor_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::disk_monitor::DiskMonitorConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_disk_monitor_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Check disk space now
async fn check_disk_space_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let status = state.manager.check_disk_space_now().await;
    let summary = state.manager.get_disk_monitor_summary().await;
    Json(serde_json::json!({
        "status": status,
        "summary": summary
    }))
}

/// Start background disk monitoring
async fn start_disk_monitor_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.start_disk_monitoring().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Stop background disk monitoring
async fn stop_disk_monitor_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.stop_disk_monitoring().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get global budget configuration and summary
async fn get_global_budget_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_global_budget_config().await;
    let summary = state.manager.get_global_budget_summary().await;
    Json(serde_json::json!({
        "config": config,
        "summary": {
            "status": format!("{}", summary.status),
            "downloads_paused": summary.downloads_paused,
            "weekly": {
                "bytes_downloaded": summary.weekly.bytes_downloaded,
                "limit_bytes": summary.weekly.limit_bytes,
                "usage_percent": summary.weekly.usage_percent,
                "remaining": summary.weekly.remaining,
                "period_start": summary.weekly.period_start.to_string(),
                "period_end": summary.weekly.period_end.to_string(),
                "status": format!("{}", summary.weekly.status)
            },
            "monthly": {
                "bytes_downloaded": summary.monthly.bytes_downloaded,
                "limit_bytes": summary.monthly.limit_bytes,
                "usage_percent": summary.monthly.usage_percent,
                "remaining": summary.monthly.remaining,
                "period_start": summary.monthly.period_start.to_string(),
                "period_end": summary.monthly.period_end.to_string(),
                "status": format!("{}", summary.monthly.status)
            }
        }
    }))
}

/// Set global budget configuration
async fn set_global_budget_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::global_budget::GlobalBudgetConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_global_budget_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get global budget usage summary
async fn global_budget_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_global_budget_summary().await;
    Json(serde_json::json!({
        "status": format!("{}", summary.status),
        "downloads_paused": summary.downloads_paused,
        "weekly": {
            "bytes_downloaded": summary.weekly.bytes_downloaded,
            "limit_bytes": summary.weekly.limit_bytes,
            "usage_percent": summary.weekly.usage_percent,
            "remaining": summary.weekly.remaining,
            "period_start": summary.weekly.period_start.to_string(),
            "period_end": summary.weekly.period_end.to_string()
        },
        "monthly": {
            "bytes_downloaded": summary.monthly.bytes_downloaded,
            "limit_bytes": summary.monthly.limit_bytes,
            "usage_percent": summary.monthly.usage_percent,
            "remaining": summary.monthly.remaining,
            "period_start": summary.monthly.period_start.to_string(),
            "period_end": summary.monthly.period_end.to_string()
        }
    }))
}

/// Reset global budget usage data
async fn reset_global_budget_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.reset_global_budget_usage().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Resume downloads after budget was exceeded
async fn resume_global_budget_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.resume_global_budget_downloads().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get retry quota usage
async fn get_retry_quota_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let usage = state.manager.get_retry_quota_usage().await;
    let config = state.manager.get_retry_quota_config().await;
    Json(serde_json::json!({
        "config": config,
        "usage": {
            "enabled": usage.enabled,
            "used": usage.used,
            "limit": usage.limit,
            "remaining": usage.remaining,
            "window_secs": usage.window_secs
        }
    }))
}

/// Set retry quota configuration
async fn set_retry_quota_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::retry_quota::RetryQuotaConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_retry_quota_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Reset retry quota
async fn reset_retry_quota_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.reset_retry_quota().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get TTL configuration
async fn get_ttl_handler(State(state): State<Arc<WebState>>) -> impl axum::response::IntoResponse {
    let config = state.manager.get_ttl_config().await;
    Json(serde_json::json!({
        "config": config,
        "enabled": config.enabled,
        "default_max_lifetime_secs": config.default_max_lifetime_secs,
        "check_interval_secs": config.check_interval_secs
    }))
}

/// Set TTL configuration
async fn set_ttl_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::ttl::TtlConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_ttl_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get TTL summary
async fn get_ttl_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_ttl_summary().await;
    Json(serde_json::json!({
        "summary": summary
    }))
}

/// Check and enforce TTL
async fn check_ttl_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.check_and_enforce_ttl().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Get error recovery configuration
async fn get_error_recovery_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_error_recovery_config().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "category_strategies": config.category_strategies,
        "max_consecutive_failures": config.max_consecutive_failures,
        "auto_switch_mirror": config.auto_switch_mirror,
    }))
}

/// Set error recovery configuration
async fn set_error_recovery_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::error_recovery::ErrorRecoveryConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_error_recovery_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "error": e.to_string()})),
    }
}

/// Classify an error and determine recovery strategy
async fn classify_error_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let error_msg = req.get("error").and_then(|v| v.as_str()).unwrap_or("");
    let consecutive_failures = req
        .get("consecutive_failures")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let decision = state
        .manager
        .classify_error(error_msg, consecutive_failures)
        .await;
    Json(serde_json::json!({
        "category": decision.category,
        "strategy": decision.strategy,
        "explanation": decision.explanation,
        "overridden": decision.overridden,
    }))
}

/// Get connection health configuration
async fn get_connection_health_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_connection_health_config().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "stall_threshold_bps": config.stall_threshold_bps,
        "degraded_stall_threshold": config.degraded_stall_threshold,
        "unhealthy_stall_threshold": config.unhealthy_stall_threshold,
        "degraded_error_threshold": config.degraded_error_threshold,
        "unhealthy_error_threshold": config.unhealthy_error_threshold,
        "degraded_timeout_threshold": config.degraded_timeout_threshold,
        "unhealthy_timeout_threshold": config.unhealthy_timeout_threshold,
        "max_idle_secs": config.max_idle_secs,
        "max_connections_per_task": config.max_connections_per_task,
        "max_total_connections": config.max_total_connections,
    }))
}

/// Set connection health configuration
async fn set_connection_health_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::connection_health::ConnectionHealthConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_connection_health_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "error": e.to_string()})),
    }
}

/// Get connection health summary
async fn get_connection_health_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_connection_health_summary().await;
    Json(serde_json::json!({
        "total_connections": summary.total_connections,
        "healthy_count": summary.healthy_count,
        "degraded_count": summary.degraded_count,
        "unhealthy_count": summary.unhealthy_count,
        "unknown_count": summary.unknown_count,
        "stale_count": summary.stale_count,
        "total_bytes_transferred": summary.total_bytes_transferred,
        "total_errors": summary.total_errors,
        "total_timeouts": summary.total_timeouts,
        "connections_needing_action": summary.connections_needing_action.len(),
    }))
}

/// Get unhealthy connections
async fn get_unhealthy_connections_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let unhealthy = state.manager.get_unhealthy_connections().await;
    Json(serde_json::json!({
        "unhealthy_connections": unhealthy,
        "count": unhealthy.len(),
    }))
}

/// Get source rotation configuration
async fn get_source_rotation_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_source_rotation_config().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "unhealthy_threshold": config.unhealthy_threshold,
        "healthy_threshold": config.healthy_threshold,
        "max_sources_per_task": config.max_sources_per_task,
        "failure_cooldown_secs": config.failure_cooldown_secs,
        "backoff_multiplier": config.backoff_multiplier,
        "max_cooldown_secs": config.max_cooldown_secs,
        "min_active_sources": config.min_active_sources,
        "auto_promote_backups": config.auto_promote_backups,
        "max_parallel_sources": config.max_parallel_sources,
    }))
}

/// Set source rotation configuration
async fn set_source_rotation_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::source_rotation::SourceRotationConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_source_rotation_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "error": e.to_string()})),
    }
}

/// Get source rotation summary for all tasks
async fn get_source_rotation_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summaries = state.manager.get_overall_source_rotation_summary().await;
    Json(serde_json::json!({
        "summaries": summaries,
    }))
}

/// Execute source rotation for all tasks
async fn execute_source_rotation_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let decisions = state.manager.execute_source_rotation_all().await;
    Json(serde_json::json!({
        "decisions": decisions,
    }))
}

/// Get bandwidth allocation configuration
async fn get_bandwidth_allocation_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_allocation_config().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "strategy": config.strategy,
        "min_bandwidth_bps": config.min_bandwidth_bps,
        "max_bandwidth_bps": config.max_bandwidth_bps,
        "recalc_interval_secs": config.recalc_interval_secs,
    }))
}

/// Set bandwidth allocation configuration
async fn set_bandwidth_allocation_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::bandwidth_allocation::AllocationConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_allocation_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "error": e.to_string()})),
    }
}

/// Get current bandwidth allocation plan
async fn get_bandwidth_allocation_plan_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_allocation_plan().await {
        Some(plan) => Json(serde_json::json!({
            "status": "ok",
            "plan": plan,
        })),
        None => Json(serde_json::json!({
            "status": "ok",
            "plan": null,
            "message": "No allocation plan calculated yet",
        })),
    }
}

/// Start a speed burst for a task
async fn start_speed_burst_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<StartBurstRequest>,
) -> impl axum::response::IntoResponse {
    let result = state
        .manager
        .start_speed_burst(&req.task_id, req.duration_secs, req.multiplier)
        .await;
    match result {
        crate::speed_burst::BurstStartResult::Started(burst) => {
            Json(serde_json::json!({"status": "started", "burst": burst}))
        }
        crate::speed_burst::BurstStartResult::Disabled => {
            Json(serde_json::json!({"status": "error", "error": "Speed burst feature is disabled"}))
        }
        crate::speed_burst::BurstStartResult::TaskNotFound => {
            Json(serde_json::json!({"status": "error", "error": "Task not found"}))
        }
        crate::speed_burst::BurstStartResult::TaskNotActive => Json(
            serde_json::json!({"status": "error", "error": "Task is not in a downloadable state"}),
        ),
        crate::speed_burst::BurstStartResult::MaxBurstsReached => Json(
            serde_json::json!({"status": "error", "error": "Maximum concurrent bursts reached"}),
        ),
        crate::speed_burst::BurstStartResult::InvalidParams(msg) => {
            Json(serde_json::json!({"status": "error", "error": msg}))
        }
    }
}

/// Stop a speed burst for a task
async fn stop_speed_burst_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<StopBurstRequest>,
) -> impl axum::response::IntoResponse {
    let stopped = state.manager.stop_speed_burst(&req.task_id).await;
    if stopped {
        Json(serde_json::json!({"status": "stopped"}))
    } else {
        Json(serde_json::json!({"status": "error", "error": "No active burst for this task"}))
    }
}

#[derive(serde::Deserialize)]
struct StartBurstRequest {
    task_id: String,
    duration_secs: Option<u64>,
    multiplier: Option<f64>,
}

#[derive(serde::Deserialize)]
struct StopBurstRequest {
    task_id: String,
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

/// Get speed prediction summary across all tracked domains
async fn get_speed_prediction_summary(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_speed_prediction_summary().await;
    Json(summary)
}

/// Set speed prediction configuration
async fn set_speed_prediction_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_prediction::SpeedPredictionConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_speed_prediction_config(config).await;
    Json(serde_json::json!({ "status": "ok" }))
}

/// Request body for speed prediction
#[derive(Debug, Deserialize)]
struct PredictSpeedRequest {
    task_id: String,
    domain: String,
    current_speed: f64,
    remaining_bytes: u64,
}

/// Predict download speed for a task
async fn predict_task_speed_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<PredictSpeedRequest>,
) -> impl axum::response::IntoResponse {
    let prediction = state
        .manager
        .predict_task_speed(
            &req.task_id,
            &req.domain,
            req.current_speed,
            req.remaining_bytes,
        )
        .await;
    Json(prediction)
}

/// Get optimal speed windows for a domain
async fn get_optimal_speed_windows(
    State(state): State<Arc<WebState>>,
    Path(domain): Path<String>,
) -> impl axum::response::IntoResponse {
    let windows = state.manager.get_optimal_speed_windows(&domain, 5).await;
    Json(windows)
}

/// Get speed profile for a domain
async fn get_domain_speed_profile(
    State(state): State<Arc<WebState>>,
    Path(domain): Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_domain_speed_profile(&domain).await {
        Some(profile) => Json(serde_json::json!({ "found": true, "profile": profile })),
        None => Json(serde_json::json!({ "found": false, "profile": null })),
    }
}

/// List all tracked speed domains
async fn list_tracked_speed_domains(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let domains = state.manager.list_tracked_speed_domains().await;
    Json(domains)
}

/// Remove a domain from speed prediction tracking
async fn remove_speed_prediction_domain(
    State(state): State<Arc<WebState>>,
    Path(domain): Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_speed_prediction_domain(&domain).await;
    Json(serde_json::json!({ "removed": removed }))
}

/// Clean up old speed prediction samples
async fn cleanup_old_speed_predictions(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.cleanup_old_speed_predictions().await;
    Json(serde_json::json!({ "status": "ok" }))
}

/// Clear all speed prediction data
async fn clear_all_speed_predictions(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_all_speed_predictions().await;
    Json(serde_json::json!({ "status": "ok" }))
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

// ─── Phase 80: Bulk Operations ───

/// Request for bulk tag operations
#[derive(Debug, Deserialize)]
pub struct BulkTagsRequest {
    pub filter: crate::bulk_ops::BulkFilter,
    pub action: crate::bulk_ops::BulkTagAction,
}

/// Request for bulk group operations
#[derive(Debug, Deserialize)]
pub struct BulkGroupRequest {
    pub filter: crate::bulk_ops::BulkFilter,
    pub action: crate::bulk_ops::BulkGroupAction,
}

/// Request for bulk priority operations
#[derive(Debug, Deserialize)]
pub struct BulkPriorityRequest {
    pub filter: crate::bulk_ops::BulkFilter,
    pub action: crate::bulk_ops::BulkPriorityAction,
}

/// Request for bulk speed limit operations
#[derive(Debug, Deserialize)]
pub struct BulkSpeedLimitRequest {
    pub filter: crate::bulk_ops::BulkFilter,
    pub action: crate::bulk_ops::BulkSpeedLimitAction,
}

/// Request for bulk weight operations
#[derive(Debug, Deserialize)]
pub struct BulkWeightRequest {
    pub filter: crate::bulk_ops::BulkFilter,
    pub action: crate::bulk_ops::BulkWeightAction,
}

async fn bulk_tags(
    State(state): State<Arc<WebState>>,
    Json(req): Json<BulkTagsRequest>,
) -> Json<crate::bulk_ops::BulkResult> {
    Json(state.manager.bulk_tag(&req.filter, &req.action).await)
}

async fn bulk_group(
    State(state): State<Arc<WebState>>,
    Json(req): Json<BulkGroupRequest>,
) -> Json<crate::bulk_ops::BulkResult> {
    Json(state.manager.bulk_group(&req.filter, &req.action).await)
}

async fn bulk_priority(
    State(state): State<Arc<WebState>>,
    Json(req): Json<BulkPriorityRequest>,
) -> Json<crate::bulk_ops::BulkResult> {
    Json(state.manager.bulk_priority(&req.filter, &req.action).await)
}

async fn bulk_speed_limit(
    State(state): State<Arc<WebState>>,
    Json(req): Json<BulkSpeedLimitRequest>,
) -> Json<crate::bulk_ops::BulkResult> {
    Json(
        state
            .manager
            .bulk_speed_limit(&req.filter, &req.action)
            .await,
    )
}

async fn bulk_weight(
    State(state): State<Arc<WebState>>,
    Json(req): Json<BulkWeightRequest>,
) -> Json<crate::bulk_ops::BulkResult> {
    Json(
        state
            .manager
            .bulk_bandwidth_weight(&req.filter, &req.action)
            .await,
    )
}

async fn bulk_match(
    State(state): State<Arc<WebState>>,
    Json(filter): Json<crate::bulk_ops::BulkFilter>,
) -> Json<Vec<String>> {
    Json(state.manager.get_bulk_filter_matches(&filter).await)
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

/// GET /api/queue-completion — Predict queue completion time
async fn get_queue_completion_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let prediction = state.manager.predict_queue_completion().await;
    Json(
        serde_json::to_value(prediction)
            .unwrap_or(serde_json::json!({"error": "Failed to serialize prediction"})),
    )
}

/// POST /api/queue-completion — Update queue completion config
async fn set_queue_completion_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::queue_completion::QueueCompletionConfig>,
) -> Json<serde_json::Value> {
    state
        .manager
        .set_queue_completion_config(config.clone())
        .await;
    Json(serde_json::json!({"status": "ok", "config": config}))
}

// ── Download Quota API Handlers (Phase 115) ──

/// GET /api/download-quota — Get quota system configuration
async fn get_download_quota_config(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_download_quota_config().await;
    Json(
        serde_json::to_value(config)
            .unwrap_or(serde_json::json!({"error": "Failed to serialize config"})),
    )
}

/// POST /api/download-quota — Update quota system configuration
async fn set_download_quota_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_quota::QuotaSystemConfig>,
) -> Json<serde_json::Value> {
    state
        .manager
        .set_download_quota_config(config.clone())
        .await;
    Json(serde_json::json!({"status": "ok", "config": config}))
}

/// GET /api/download-quota/summary — Get quota summary with usage statistics
async fn get_download_quota_summary(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_download_quota_summary().await;
    Json(
        serde_json::to_value(summary)
            .unwrap_or(serde_json::json!({"error": "Failed to serialize summary"})),
    )
}

/// GET /api/download-quota/rules — List all quota rules
async fn list_download_quota_rules(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let rules = state.manager.list_download_quota_rules().await;
    Json(
        serde_json::to_value(rules)
            .unwrap_or(serde_json::json!({"error": "Failed to serialize rules"})),
    )
}

/// POST /api/download-quota/rules — Add a new quota rule
async fn add_download_quota_rule(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::download_quota::QuotaRule>,
) -> Json<serde_json::Value> {
    match state.manager.add_download_quota_rule(rule).await {
        Ok(rule_id) => Json(serde_json::json!({"status": "ok", "rule_id": rule_id})),
        Err(e) => Json(serde_json::json!({"status": "error", "message": e.to_string()})),
    }
}

/// POST /api/download-quota/rules/:id/remove — Remove a quota rule
async fn remove_download_quota_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let removed = state.manager.remove_download_quota_rule(&id).await;
    if removed {
        Json(serde_json::json!({"status": "ok", "removed": true}))
    } else {
        Json(serde_json::json!({"status": "error", "message": "Rule not found"}))
    }
}

/// POST /api/download-quota/rules/:id/enable — Enable or disable a quota rule
async fn set_download_quota_rule_enabled(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let updated = state
        .manager
        .set_download_quota_rule_enabled(&id, enabled)
        .await;
    if updated {
        Json(serde_json::json!({"status": "ok", "enabled": enabled}))
    } else {
        Json(serde_json::json!({"status": "error", "message": "Rule not found"}))
    }
}

/// POST /api/download-quota/refresh — Refresh all quota usage (reset for new day)
async fn refresh_download_quota(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.refresh_download_quota().await;
    Json(serde_json::json!({"status": "ok", "message": "Quota usage refreshed"}))
}

/// POST /api/download-quota/clear — Clear all quota usage data
async fn clear_download_quota_usage(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_download_quota_usage().await;
    Json(serde_json::json!({"status": "ok", "message": "Quota usage cleared"}))
}

// ========== Phase 117: Advanced Search API ==========

/// POST /api/search — Execute an advanced search query
async fn advanced_search_handler(
    State(state): State<Arc<WebState>>,
    body: axum::body::Bytes,
) -> Json<serde_json::Value> {
    // Parse query from JSON manually (AdvancedSearchQuery doesn't derive Deserialize)
    let json: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return Json(serde_json::json!({"error": format!("Invalid JSON: {}", e)}));
        }
    };

    let query = parse_search_query(&json);

    // Save as last query
    state.manager.set_last_search_query(query.clone()).await;

    // Execute search
    let result = state
        .manager
        .advanced_search(&query, None, Some(100), None)
        .await;

    Json(task_result_to_json(&result))
}

/// GET /api/search/quick/:query — Quick name substring search
async fn quick_search_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(query): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let tasks = state.manager.quick_search(&query).await;
    Json(serde_json::json!({
        "total": tasks.len(),
        "tasks": tasks.iter().map(|t| task_to_brief_json(t)).collect::<Vec<_>>()
    }))
}

/// GET /api/search/stats — Get search statistics
async fn search_stats_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let stats = state.manager.get_search_stats().await;
    Json(stats)
}

/// POST /api/search/last — Re-run the last search query
async fn rerun_last_search_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let result = state.manager.rerun_last_search(None, Some(100), None).await;
    Json(task_result_to_json(&result))
}

/// Helper: convert a task to a brief JSON representation
fn task_to_brief_json(t: &crate::DownloadTask) -> serde_json::Value {
    serde_json::json!({
        "id": t.id,
        "name": t.name,
        "state": format!("{:?}", t.state),
        "protocol": format!("{:?}", t.protocol),
        "size": t.size,
        "downloaded": t.downloaded,
        "speed_bps": t.speed_bps,
        "tags": t.tags,
        "group": t.group,
        "priority": format!("{:?}", t.priority),
        "created_at": t.created_at.to_rfc3339(),
        "updated_at": t.updated_at.to_rfc3339(),
        "has_notes": t.notes.is_some(),
        "has_error": t.error.is_some(),
        "has_mirrors": !t.mirror_urls.is_empty(),
        "has_deadline": t.deadline.is_some(),
        "has_checksum": t.expected_checksum.is_some()
    })
}

/// Helper: convert SearchResult to JSON
fn task_result_to_json(result: &crate::advanced_search::SearchResult) -> serde_json::Value {
    serde_json::json!({
        "total": result.total,
        "execution_time_us": result.execution_time_us,
        "query_summary": result.query_summary,
        "tasks": result.tasks.iter().map(|t| task_to_brief_json(t)).collect::<Vec<_>>()
    })
}

/// Helper: parse a JSON value into an AdvancedSearchQuery
fn parse_search_query(json: &serde_json::Value) -> crate::advanced_search::AdvancedSearchQuery {
    let mut query = crate::advanced_search::AdvancedSearchQuery::new();

    if let Some(s) = json.get("name_contains").and_then(|v| v.as_str()) {
        query.name_contains = Some(s.to_string());
    }
    if let Some(s) = json.get("name_regex").and_then(|v| v.as_str()) {
        query.name_regex = Some(s.to_string());
    }
    if let Some(s) = json.get("group").and_then(|v| v.as_str()) {
        query.group = Some(s.to_string());
    }
    if let Some(s) = json.get("tag").and_then(|v| v.as_str()) {
        query.tag = Some(s.to_string());
    }
    if let Some(arr) = json.get("tags_any").and_then(|v| v.as_array()) {
        query.tags_any = Some(
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
        );
    }
    if let Some(arr) = json.get("tags_all").and_then(|v| v.as_array()) {
        query.tags_all = Some(
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
        );
    }
    if let Some(n) = json.get("min_size").and_then(|v| v.as_u64()) {
        query.min_size = Some(n);
    }
    if let Some(n) = json.get("max_size").and_then(|v| v.as_u64()) {
        query.max_size = Some(n);
    }
    if let Some(n) = json.get("min_progress").and_then(|v| v.as_f64()) {
        query.min_progress = Some(n);
    }
    if let Some(n) = json.get("max_progress").and_then(|v| v.as_f64()) {
        query.max_progress = Some(n);
    }
    if let Some(n) = json.get("min_speed").and_then(|v| v.as_f64()) {
        query.min_speed = Some(n);
    }
    if let Some(n) = json.get("max_speed").and_then(|v| v.as_f64()) {
        query.max_speed = Some(n);
    }
    if let Some(b) = json.get("has_tags").and_then(|v| v.as_bool()) {
        query.has_tags = Some(b);
    }
    if let Some(b) = json.get("has_notes").and_then(|v| v.as_bool()) {
        query.has_notes = Some(b);
    }
    if let Some(b) = json.get("has_error").and_then(|v| v.as_bool()) {
        query.has_error = Some(b);
    }
    if let Some(b) = json.get("has_mirrors").and_then(|v| v.as_bool()) {
        query.has_mirrors = Some(b);
    }
    if let Some(b) = json.get("has_deadline").and_then(|v| v.as_bool()) {
        query.has_deadline = Some(b);
    }
    if let Some(b) = json.get("has_checksum").and_then(|v| v.as_bool()) {
        query.has_checksum = Some(b);
    }
    if let Some(b) = json.get("has_speed_limit").and_then(|v| v.as_bool()) {
        query.has_speed_limit = Some(b);
    }
    if let Some(b) = json.get("in_queue").and_then(|v| v.as_bool()) {
        query.in_queue = Some(b);
    }
    if let Some(b) = json.get("is_active").and_then(|v| v.as_bool()) {
        query.is_active = Some(b);
    }
    if let Some(b) = json.get("is_complete").and_then(|v| v.as_bool()) {
        query.is_complete = Some(b);
    }
    if let Some(b) = json.get("is_failed").and_then(|v| v.as_bool()) {
        query.is_failed = Some(b);
    }
    if let Some(b) = json.get("is_paused").and_then(|v| v.as_bool()) {
        query.is_paused = Some(b);
    }
    // State filter
    if let Some(s) = json.get("state").and_then(|v| v.as_str()) {
        query.state = match s.to_lowercase().as_str() {
            "queued" => Some(crate::DownloadState::Queued),
            "downloading" => Some(crate::DownloadState::Downloading),
            "paused" => Some(crate::DownloadState::Paused),
            "complete" | "completed" => Some(crate::DownloadState::Complete),
            "error" | "failed" => Some(crate::DownloadState::Error),
            _ => None,
        };
    }
    // Priority filter
    if let Some(s) = json.get("priority").and_then(|v| v.as_str()) {
        query.priority = match s.to_lowercase().as_str() {
            "low" => Some(crate::DownloadPriority::Low),
            "normal" => Some(crate::DownloadPriority::Normal),
            "high" => Some(crate::DownloadPriority::High),
            _ => None,
        };
    }
    if let Some(s) = json.get("min_priority").and_then(|v| v.as_str()) {
        query.min_priority = match s.to_lowercase().as_str() {
            "low" => Some(crate::DownloadPriority::Low),
            "normal" => Some(crate::DownloadPriority::Normal),
            "high" => Some(crate::DownloadPriority::High),
            _ => None,
        };
    }

    query
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

// ─── Phase 98: Download Queue Snapshot Handlers ───

#[derive(serde::Deserialize)]
struct CreateSnapshotRequest {
    name: String,
    description: Option<String>,
}

/// List all available snapshots
async fn list_snapshots_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::download_snapshot::SnapshotSummary>> {
    let summaries = state.manager.list_queue_snapshots().await;
    Json(summaries)
}

/// Create a new snapshot
async fn create_snapshot_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<CreateSnapshotRequest>,
) -> impl axum::response::IntoResponse {
    match state
        .manager
        .create_queue_snapshot(req.name, req.description)
        .await
    {
        Ok(entry) => (
            axum::http::StatusCode::CREATED,
            Json(serde_json::json!({
                "status": "created",
                "id": entry.id,
                "name": entry.name,
                "task_count": entry.task_count,
                "total_size": entry.total_size,
            })),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// Get a specific snapshot's data
async fn get_snapshot_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_queue_snapshot(&id).await {
        Ok(data) => Json(serde_json::json!({
            "id": data.id,
            "name": data.name,
            "description": data.description,
            "created_at": data.created_at.to_rfc3339(),
            "global_speed_limit": data.global_speed_limit,
            "max_concurrent": data.max_concurrent,
            "task_count": data.tasks.len(),
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Restore a snapshot
async fn restore_snapshot_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.restore_queue_snapshot(&id).await {
        Ok(tasks) => Json(serde_json::json!({
            "status": "restored",
            "task_count": tasks.len(),
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Delete a snapshot
async fn delete_snapshot_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.delete_queue_snapshot(&id).await {
        Ok(()) => Json(serde_json::json!({"status": "deleted"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

// ─── Phase 103: Network Monitor & Download Time Limit REST API ───

/// GET /api/network-monitor - Get network monitor status and summary
async fn get_network_monitor_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let summary = state.manager.get_network_summary().await;
    let config = state.manager.get_network_monitor_config().await;
    Json(serde_json::json!({
        "config": config,
        "summary": summary
    }))
}

/// POST /api/network-monitor - Update network monitor configuration
async fn set_network_monitor_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::network_monitor::NetworkMonitorConfig>,
) -> impl IntoResponse {
    state.manager.set_network_monitor_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/network-monitor/clear - Clear network monitor data
async fn clear_network_monitor_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    state.manager.clear_network_monitor().await;
    Json(serde_json::json!({"status": "cleared"}))
}

/// GET /api/download-time-limit - Get download time limit configuration
async fn get_download_time_limit_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let config = state.manager.get_download_time_limit_config().await;
    Json(config)
}

/// POST /api/download-time-limit - Update download time limit configuration
async fn set_download_time_limit_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_time_limit::DownloadTimeLimitConfig>,
) -> impl IntoResponse {
    state.manager.set_download_time_limit_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
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
            "Failed to apply preset (not found, disabled, or task not found)".to_string()
        },
        task_id: Some(task_id),
    })
}

/// PUT /api/download-presets/:id - Update a preset
async fn update_download_preset(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(updates): Json<crate::download_presets::PresetUpdate>,
) -> Json<TaskResponse> {
    let updated = state.manager.update_download_preset(&id, updates).await;
    Json(TaskResponse {
        success: updated,
        message: if updated {
            format!("Updated preset {}", id)
        } else {
            format!("Preset {} not found", id)
        },
        task_id: None,
    })
}

/// POST /api/download-presets/:id/enable - Enable a preset
async fn enable_download_preset(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let enabled = state.manager.enable_download_preset(&id).await;
    Json(TaskResponse {
        success: enabled,
        message: if enabled {
            format!("Enabled preset {}", id)
        } else {
            format!("Preset {} not found", id)
        },
        task_id: None,
    })
}

/// POST /api/download-presets/:id/disable - Disable a preset
async fn disable_download_preset(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<TaskResponse> {
    let disabled = state.manager.disable_download_preset(&id).await;
    Json(TaskResponse {
        success: disabled,
        message: if disabled {
            format!("Disabled preset {}", id)
        } else {
            format!("Preset {} not found", id)
        },
        task_id: None,
    })
}

/// GET /api/download-presets/categories - Get all preset categories
async fn get_preset_categories(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let categories = state.manager.get_preset_categories().await;
    Json(serde_json::json!({
        "categories": categories,
        "count": categories.len()
    }))
}

/// GET /api/download-presets/category/:category - List presets by category
async fn list_presets_by_category(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(category): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let presets = state.manager.list_presets_by_category(&category).await;
    Json(serde_json::json!({
        "category": category,
        "presets": presets,
        "count": presets.len()
    }))
}

/// GET /api/download-presets/usage-summary - Get preset usage statistics
async fn get_preset_usage_summary(
    State(state): State<Arc<WebState>>,
) -> Json<crate::download_presets::PresetUsageSummary> {
    let summary = state.manager.get_preset_usage_summary().await;
    Json(summary)
}

// ========== URL Bookmarks REST API ==========

/// Request to add URLs to a bookmark
#[derive(Debug, Deserialize)]
struct AddUrlsRequest {
    urls: Vec<String>,
}

/// Request to remove a URL from a bookmark
#[derive(Debug, Deserialize)]
struct RemoveUrlRequest {
    url: String,
}

/// GET /api/url-bookmarks - List all bookmarks
async fn list_url_bookmarks(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let bookmarks = state.manager.list_url_bookmarks().await;
    let summaries: Vec<crate::url_bookmarks::BookmarkSummary> = bookmarks
        .iter()
        .map(crate::url_bookmarks::BookmarkSummary::from)
        .collect();
    Json(serde_json::json!({
        "bookmarks": summaries,
        "count": summaries.len()
    }))
}

/// POST /api/url-bookmarks - Add a bookmark
async fn add_url_bookmark(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let name = req
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let urls: Vec<String> = req
        .get("urls")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let entries: Vec<crate::url_bookmarks::BookmarkEntry> = urls
        .iter()
        .map(crate::url_bookmarks::BookmarkEntry::new)
        .collect();

    match state.manager.add_url_bookmark(name, entries).await {
        Ok(bm) => Ok(Json(serde_json::json!({
            "success": true,
            "bookmark": {
                "id": bm.id,
                "name": bm.name,
                "url_count": bm.entries.len(),
            }
        }))),
        Err(_) => Err(StatusCode::BAD_REQUEST),
    }
}

/// GET /api/url-bookmarks/:name - Get a bookmark
async fn get_url_bookmark(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.get_url_bookmark(&name).await {
        Some(bm) => Ok(Json(serde_json::json!({
            "id": bm.id,
            "name": bm.name,
            "entries": bm.entries,
            "description": bm.description,
            "created_at": bm.created_at,
            "last_used_at": bm.last_used_at,
            "import_count": bm.import_count,
            "enabled": bm.enabled,
        }))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/url-bookmarks/:name - Remove a bookmark
async fn remove_url_bookmark(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<TaskResponse>, StatusCode> {
    match state.manager.remove_url_bookmark(&name).await {
        Ok(()) => Ok(Json(TaskResponse {
            success: true,
            message: format!("Removed bookmark '{}'", name),
            task_id: None,
        })),
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/url-bookmarks/:name/import - Import all URLs as tasks
async fn import_url_bookmark(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.import_bookmark(&name).await {
        Ok(result) => Ok(Json(serde_json::json!({
            "success": true,
            "bookmark_name": result.bookmark_name,
            "urls_imported": result.urls_imported,
            "urls_skipped": result.urls_skipped,
        }))),
        Err(e) => Ok(Json(serde_json::json!({
            "success": false,
            "error": e,
        }))),
    }
}

/// POST /api/url-bookmarks/:name/urls - Add URLs to a bookmark
async fn add_urls_to_bookmark(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<AddUrlsRequest>,
) -> Result<Json<TaskResponse>, StatusCode> {
    let entries: Vec<crate::url_bookmarks::BookmarkEntry> = req
        .urls
        .iter()
        .map(crate::url_bookmarks::BookmarkEntry::new)
        .collect();
    match state.manager.add_urls_to_bookmark(&name, entries).await {
        Ok(()) => Ok(Json(TaskResponse {
            success: true,
            message: format!("Added URLs to bookmark '{}'", name),
            task_id: None,
        })),
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/url-bookmarks/:name/urls/remove - Remove a URL from a bookmark
async fn remove_url_from_bookmark(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<RemoveUrlRequest>,
) -> Result<Json<TaskResponse>, StatusCode> {
    match state
        .manager
        .remove_url_from_bookmark(&name, &req.url)
        .await
    {
        Ok(()) => Ok(Json(TaskResponse {
            success: true,
            message: format!("Removed URL from bookmark '{}'", name),
            task_id: None,
        })),
        Err(_) => Err(StatusCode::NOT_FOUND),
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

/// GET /api/auto-actions - Get auto-actions configuration
async fn get_auto_actions_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let config = state.manager.get_auto_actions_config().await;
    Json(config)
}

/// POST /api/auto-actions - Update auto-actions configuration
async fn set_auto_actions_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::auto_actions::AutoActionsConfig>,
) -> impl IntoResponse {
    state.manager.set_auto_actions_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/auto-actions/summary - Get auto-actions summary
async fn get_auto_actions_summary_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let summary = state.manager.get_auto_actions_summary().await;
    Json(summary)
}

/// GET /api/auto-actions/rules - List all auto-action rules
async fn list_auto_action_rules_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let rules = state.manager.list_auto_action_rules().await;
    Json(rules)
}

/// POST /api/auto-actions/rules - Add a new auto-action rule
async fn add_auto_action_rule_handler(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::auto_actions::AutoActionRule>,
) -> impl IntoResponse {
    let id = state.manager.add_auto_action_rule(rule).await;
    Json(serde_json::json!({"status": "ok", "rule_id": id}))
}

/// DELETE /api/auto-actions/rules/:id - Remove an auto-action rule
async fn remove_auto_action_rule_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.manager.remove_auto_action_rule(&id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "message": e.to_string()})),
    }
}

/// POST /api/auto-actions/rules/:id/enable - Enable or disable an auto-action rule
async fn set_auto_action_rule_enabled_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match state
        .manager
        .set_auto_action_rule_enabled(&id, enabled)
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "message": e.to_string()})),
    }
}

/// POST /api/auto-actions/task/:task_id - Set per-task auto-action override
async fn set_task_auto_action_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let actions: Vec<crate::auto_actions::AutoAction> = match body.get("actions") {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(a) => a,
            Err(e) => {
                return Json(serde_json::json!({"status": "error", "message": e.to_string()}));
            }
        },
        None => return Json(serde_json::json!({"status": "error", "message": "missing actions"})),
    };
    let trigger: crate::auto_actions::AutoActionTrigger = body
        .get("trigger")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    state
        .manager
        .set_task_auto_action(&task_id, actions, trigger)
        .await;
    Json(serde_json::json!({"status": "ok"}))
}

/// DELETE /api/auto-actions/task/:task_id - Remove per-task auto-action override
async fn remove_task_auto_action_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    match state.manager.remove_task_auto_action(&task_id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"status": "error", "message": e.to_string()})),
    }
}

/// POST /api/auto-actions/history/clear - Clear auto-actions execution history
async fn clear_auto_actions_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    state.manager.clear_auto_actions_history().await;
    Json(serde_json::json!({"status": "cleared"}))
}

// ─── Phase 118: Automation Rules Engine API ─────────────────────────────

/// GET /api/automation — Get automation rules summary
async fn get_automation_summary(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_automation_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/automation/config — Get automation config
async fn get_automation_config(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_automation_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/automation/config — Update automation config
async fn set_automation_config(
    State(state): State<Arc<WebState>>,
    body: bytes::Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<crate::automation_rules::AutomationConfig>(&body) {
        Ok(config) => {
            state.manager.set_automation_config(config).await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/automation/rules — List all automation rules
async fn list_automation_rules(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let rules = state.manager.list_automation_rules().await;
    Json(serde_json::to_value(rules).unwrap_or_default())
}

/// POST /api/automation/rules — Add a new automation rule
async fn add_automation_rule(
    State(state): State<Arc<WebState>>,
    body: bytes::Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<crate::automation_rules::AutomationRule>(&body) {
        Ok(rule) => match state.manager.add_automation_rule(rule).await {
            Ok(id) => Json(serde_json::json!({"id": id, "status": "created"})),
            Err(e) => Json(serde_json::json!({"error": e})),
        },
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/automation/rules/:id — Get a specific automation rule
async fn get_automation_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.get_automation_rule(&id).await {
        Some(rule) => Json(serde_json::to_value(rule).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "rule not found"})),
    }
}

/// PUT /api/automation/rules/:id — Update an automation rule
async fn update_automation_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    body: bytes::Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<crate::automation_rules::AutomationRule>(&body) {
        Ok(mut rule) => {
            rule.id = id;
            let updated = state.manager.update_automation_rule(rule).await;
            if updated {
                Json(serde_json::json!({"status": "updated"}))
            } else {
                Json(serde_json::json!({"error": "rule not found"}))
            }
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// DELETE /api/automation/rules/:id — Delete an automation rule
async fn delete_automation_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let removed = state.manager.remove_automation_rule(&id).await;
    if removed {
        Json(serde_json::json!({"status": "deleted"}))
    } else {
        Json(serde_json::json!({"error": "rule not found"}))
    }
}

/// POST /api/automation/rules/:id/enable — Enable/disable a rule
async fn enable_automation_rule(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    body: bytes::Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(json) => {
            let enabled = json
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let updated = state
                .manager
                .set_automation_rule_enabled(&id, enabled)
                .await;
            if updated {
                Json(serde_json::json!({"status": "updated", "enabled": enabled}))
            } else {
                Json(serde_json::json!({"error": "rule not found"}))
            }
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/automation/history/clear — Clear fire history
async fn clear_automation_history(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    state.manager.clear_automation_history().await;
    Json(serde_json::json!({"status": "cleared"}))
}

/// POST /api/automation/counts/reset — Reset all fire counts
async fn reset_automation_counts(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    state.manager.reset_automation_counts().await;
    Json(serde_json::json!({"status": "reset"}))
}

// ─── Phase 119: Task Schedule Windows API ─────────────────────────────

/// GET /api/schedule-windows — Get summary of task schedule windows
async fn get_schedule_windows_summary(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    Json(state.manager.get_task_schedule_windows_summary().await)
}

/// GET /api/schedule-windows/config — Get task schedule windows configuration
async fn get_schedule_windows_config(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_task_schedule_windows_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/schedule-windows/config — Update task schedule windows configuration
async fn set_schedule_windows_config(
    State(state): State<Arc<WebState>>,
    body: bytes::Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<crate::task_schedule_windows::TaskScheduleWindowsConfig>(&body) {
        Ok(config) => {
            state.manager.set_task_schedule_windows_config(config).await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/schedule-windows/:task_id — Get schedule windows for a task
async fn get_task_schedule_windows(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.get_task_schedule_windows(&task_id).await {
        Some(windows) => Json(serde_json::to_value(windows).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "no schedule windows for this task"})),
    }
}

/// POST /api/schedule-windows/:task_id — Add a schedule window to a task
async fn add_task_schedule_window(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    body: bytes::Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<crate::task_schedule_windows::ScheduleWindow>(&body) {
        Ok(window) => {
            state
                .manager
                .add_task_schedule_window(&task_id, window)
                .await;
            Json(serde_json::json!({"status": "added"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/schedule-windows/:task_id/clear — Clear all schedule windows for a task
async fn clear_task_schedule_windows(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    state.manager.clear_task_schedule_windows(&task_id).await;
    Json(serde_json::json!({"status": "cleared"}))
}

/// DELETE /api/schedule-windows/:task_id/:window_id — Remove a schedule window
async fn remove_task_schedule_window(
    State(state): State<Arc<WebState>>,
    axum::extract::Path((task_id, window_id)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    let removed = state
        .manager
        .remove_task_schedule_window(&task_id, &window_id)
        .await;
    if removed {
        Json(serde_json::json!({"status": "removed"}))
    } else {
        Json(serde_json::json!({"error": "window not found"}))
    }
}

/// GET /api/schedule-windows/:task_id/check — Check if task is allowed now
async fn check_task_schedule_allowed(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    // Get task priority from tasks list
    let priority = {
        let tasks = state.manager.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .map(|t| t.priority as i32)
            .unwrap_or(0)
    };

    let allowed = state
        .manager
        .is_task_allowed_by_schedule(&task_id, priority)
        .await;
    let next_time = state
        .manager
        .next_task_allowed_time(&task_id, priority)
        .await;

    Json(serde_json::json!({
        "allowed": allowed,
        "next_allowed": next_time.map(|t| t.to_rfc3339())
    }))
}

// ========== Phase 162: Completion Probability REST API ==========

/// GET /api/completion-probability - Get completion probability configuration
async fn get_completion_probability_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<completion_probability::CompletionProbabilityConfig> {
    Json(state.manager.get_completion_probability_config().await)
}

/// POST /api/completion-probability - Update completion probability configuration
async fn set_completion_probability_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<completion_probability::CompletionProbabilityConfig>,
) -> StatusCode {
    state
        .manager
        .set_completion_probability_config(config)
        .await;
    StatusCode::OK
}

/// GET /api/completion-probability/summary - Get summary of cached estimates
async fn get_completion_probability_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<completion_probability::EstimatorSummary> {
    Json(state.manager.get_completion_probability_summary().await)
}

/// POST /api/completion-probability/estimate - Estimate completion probability for a task
async fn estimate_completion_probability_handler(
    State(state): State<Arc<WebState>>,
    Json(request): Json<EstimateProbabilityRequest>,
) -> Json<completion_probability::CompletionProbability> {
    Json(
        state
            .manager
            .estimate_completion_probability(request.input, request.signals)
            .await,
    )
}

/// GET /api/completion-probability/cache/:task_id - Get cached estimate for a task
async fn get_cached_completion_probability_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<completion_probability::CompletionProbability>, StatusCode> {
    state
        .manager
        .get_cached_completion_probability(&task_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// POST /api/completion-probability/cache - Clear all cached estimates
async fn clear_completion_probability_cache_handler(
    State(state): State<Arc<WebState>>,
) -> StatusCode {
    state.manager.clear_completion_probability_cache().await;
    StatusCode::OK
}

/// Request body for estimating completion probability
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EstimateProbabilityRequest {
    input: completion_probability::TaskProbabilityInput,
    signals: completion_probability::EstimatorSignals,
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
                is_favorite: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
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
    async fn test_queue_health_config_endpoints() {
        let state = test_state();
        let app = create_router(state);

        // Test GET /api/health/config
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/health/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let config: crate::queue_health::HealthMonitorConfig =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(config.slow_threshold_bps, 1024.0);

        // Test POST /api/health/config
        let new_config = crate::queue_health::HealthMonitorConfig {
            slow_threshold_bps: 2048.0,
            stuck_threshold_secs: 600.0,
            max_retry_threshold: 10,
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/health/config")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&new_config).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify config was updated
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/health/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let config: crate::queue_health::HealthMonitorConfig =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(config.slow_threshold_bps, 2048.0);
        assert_eq!(config.stuck_threshold_secs, 600.0);
    }

    #[tokio::test]
    async fn test_queue_staleness_endpoints() {
        let state = test_state();
        let app = create_router(state);

        // Test GET /api/queue-staleness
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queue-staleness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let summary: crate::queue_staleness::StalenessSummary =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(summary.total_queued, 0);
        assert_eq!(summary.stale_count, 0);

        // Test POST /api/queue-staleness (update config)
        let new_config = crate::queue_staleness::StalenessConfig {
            enabled: true,
            stale_threshold_secs: 1800,
            auto_promote: true,
            max_promote_priority: crate::queue_staleness::StalePriority::High,
            promote_levels: 2,
            max_promotions: 5,
            check_interval_secs: 600,
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/queue-staleness")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&new_config).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Test POST /api/queue-staleness/check
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/queue-staleness/check")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let summary: crate::queue_staleness::StalenessSummary =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(summary.config.stale_threshold_secs, 1800);

        // Test POST /api/queue-staleness/clear
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/queue-staleness/clear")
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
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
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

    // ===== Phase 158: Speed Heatmap API Tests =====

    #[tokio::test]
    async fn test_speed_heatmap_config_api() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/speed-heatmap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_speed_heatmap_summary_api() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/speed-heatmap/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_speed_heatmap_report_api() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/speed-heatmap/report")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_speed_heatmap_hourly_api() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/speed-heatmap/hourly/10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_speed_heatmap_daily_api() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/speed-heatmap/daily/2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_speed_heatmap_quality_api() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/speed-heatmap/quality/2/10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}

// ===== Source Benchmark Handlers (Phase 120) =====

/// Get source benchmark configuration
async fn get_source_benchmark_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_source_benchmark_config().await;
    Json(serde_json::json!({
        "config": config
    }))
}

/// Set source benchmark configuration
async fn set_source_benchmark_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::source_benchmark::BenchmarkConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_source_benchmark_config(config).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Run source benchmark on a list of URLs
async fn run_source_benchmark_handler(
    State(state): State<Arc<WebState>>,
    Json(request): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let urls = match request.get("urls").and_then(|v| v.as_array()) {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>(),
        None => return Json(serde_json::json!({"error": "missing 'urls' array"})),
    };

    if urls.is_empty() {
        return Json(serde_json::json!({"error": "urls array is empty"}));
    }

    match state.manager.benchmark_sources(&urls).await {
        Ok(summary) => Json(serde_json::json!({
            "status": "ok",
            "summary": {
                "total_sources": summary.total_sources,
                "successful": summary.successful,
                "failed": summary.failed,
                "fastest_url": summary.fastest_url,
                "fastest_speed_bps": summary.fastest_speed_bps,
                "slowest_speed_bps": summary.slowest_speed_bps,
                "avg_speed_bps": summary.avg_speed_bps,
                "total_duration_ms": summary.total_duration_ms,
                "results": summary.results.iter().map(|r| serde_json::json!({
                    "url": r.url,
                    "success": r.success,
                    "speed_bps": r.speed_bps,
                    "latency_ms": r.latency_ms,
                    "http_status": r.http_status,
                    "bytes_downloaded": r.bytes_downloaded,
                    "duration_ms": r.duration_ms,
                    "error": r.error
                })).collect::<Vec<_>>()
            }
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Select the best source from a list of URLs
async fn select_best_source_handler(
    State(state): State<Arc<WebState>>,
    Json(request): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let urls = match request.get("urls").and_then(|v| v.as_array()) {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>(),
        None => return Json(serde_json::json!({"error": "missing 'urls' array"})),
    };

    if urls.is_empty() {
        return Json(serde_json::json!({"error": "urls array is empty"}));
    }

    match state.manager.select_best_source(&urls).await {
        Ok(best_url) => Json(serde_json::json!({
            "status": "ok",
            "best_url": best_url
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Get source benchmark cache summary
async fn get_source_benchmark_cache_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_source_benchmark_cache_summary().await;
    Json(serde_json::json!({
        "total_domains": summary.total_domains,
        "fast_domains": summary.fast_domains,
        "slow_domains": summary.slow_domains
    }))
}

/// Clear source benchmark cache
async fn clear_source_benchmark_cache_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_source_benchmark_cache().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ==================== Phase 123: Download Backup API ====================

/// List all available backups
async fn list_backups_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    match state.manager.list_backups().await {
        Ok(backups) => Json(serde_json::json!({
            "status": "ok",
            "backups": backups.iter().map(|b| serde_json::json!({
                "path": b.path.to_string_lossy(),
                "created_at": b.created_at.to_rfc3339(),
                "description": b.description,
                "task_count": b.task_count,
                "config_count": b.config_count
            })).collect::<Vec<_>>()
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Create a new backup
async fn create_backup_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let description = body
        .get("description")
        .and_then(|v| v.as_str())
        .map(String::from);

    match state.manager.create_backup(description).await {
        Ok(path) => Json(serde_json::json!({
            "status": "ok",
            "path": path.to_string_lossy()
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Get backup details
async fn get_backup_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let backup_path = std::path::PathBuf::from(&id);
    match state.manager.load_backup(&backup_path).await {
        Ok(backup) => Json(serde_json::json!({
            "status": "ok",
            "backup": {
                "version": backup.version,
                "created_at": backup.created_at.to_rfc3339(),
                "description": backup.description,
                "source": backup.source,
                "task_count": backup.tasks.tasks.len(),
                "config_count": backup.configs.count_some()
            }
        })),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// Delete a backup
async fn delete_backup_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let backup_path = std::path::PathBuf::from(&id);
    match state.manager.delete_backup(&backup_path).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

// ── Preflight Check Handlers ─────────────────────────────────────

/// GET /api/preflight - Get preflight check configuration
async fn get_preflight_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_preflight_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/preflight - Set preflight check configuration
async fn set_preflight_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::preflight_check::PreflightConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_preflight_config(config).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/preflight/run - Run preflight checks for a URL
async fn run_preflight_handler(
    State(state): State<Arc<WebState>>,
    Json(input): Json<crate::preflight_check::PreflightInput>,
) -> impl axum::response::IntoResponse {
    let report = state.manager.run_preflight_check(input).await;
    Json(serde_json::to_value(report).unwrap_or_default())
}

// ── Cost Tracker Handlers (Phase 127) ──────────────────────────

/// GET /api/cost - Get cost tracker configuration
async fn get_cost_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_cost_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/cost - Set cost tracker configuration
async fn set_cost_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_cost::CostConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_cost_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/cost/summary - Get current month cost summary
async fn get_cost_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_cost_summary_current_month().await;
    let formatted = state.manager.format_cost_summary(&summary).await;
    Json(serde_json::json!({
        "summary": summary,
        "formatted": formatted
    }))
}

/// GET /api/cost/summary/month?date=YYYY-MM - Get monthly cost summary
async fn get_cost_monthly_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    let date = params
        .get("date")
        .cloned()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m").to_string());
    let summary = state.manager.get_cost_summary_for_date(&date).await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/cost/summary/all - Get all-time cost summary
async fn get_cost_all_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_cost_summary_all().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/cost/tasks - Get per-task cost records
async fn get_cost_tasks_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let records = state.manager.get_all_task_costs().await;
    Json(serde_json::to_value(records).unwrap_or_default())
}

/// GET /api/cost/daily - Get daily cost usage records
async fn get_cost_daily_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let records = state.manager.get_daily_cost_usage().await;
    Json(serde_json::to_value(records).unwrap_or_default())
}

/// POST /api/cost/clear - Clear all cost tracking data
async fn clear_cost_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_cost_data().await;
    Json(serde_json::json!({"status": "ok"}))
}

// --- Speed Test (Phase 128) ---

/// GET /api/speed-test - Get speed test configuration
async fn get_speed_test_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_speed_test_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/speed-test - Update speed test configuration
async fn set_speed_test_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_test::SpeedTestConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_speed_test_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/speed-test/run - Run a speed test against a URL
async fn run_speed_test_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let url = body.get("url").and_then(|v| v.as_str()).unwrap_or_default();
    if url.is_empty() {
        return Json(serde_json::json!({"error": "url is required"}));
    }
    let result = state.manager.run_speed_test(url).await;
    Json(serde_json::to_value(result).unwrap_or_default())
}

/// GET /api/speed-test/summary - Get speed test summary
async fn get_speed_test_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_speed_test_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/speed-test/history - Get speed test history
async fn get_speed_test_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let history = state.manager.get_speed_test_history().await;
    Json(serde_json::to_value(history).unwrap_or_default())
}

/// GET /api/speed-test/latest - Get latest speed test result
async fn get_speed_test_latest_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_latest_speed_test().await {
        Some(result) => Json(serde_json::to_value(result).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "no speed test recorded"})),
    }
}

/// POST /api/speed-test/clear - Clear speed test history
async fn clear_speed_test_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_speed_test_history().await;
    Json(serde_json::json!({"status": "ok"}))
}

// --- Speed Trend Analysis (Phase 138) ---

/// GET /api/speed-trend - Get speed trend configuration
async fn get_speed_trend_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_speed_trend_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/speed-trend - Update speed trend configuration
async fn set_speed_trend_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_trend::SpeedTrendConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_speed_trend_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/speed-trend/summary - Get speed trend summary
async fn get_speed_trend_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_speed_trend_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/speed-trend/trends - Get all domain trends
async fn get_all_speed_trends_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let trends = state.manager.get_all_speed_trends().await;
    Json(serde_json::to_value(trends).unwrap_or_default())
}

/// GET /api/speed-trend/degrading - Get domains with degrading trends
async fn get_degrading_trends_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let trends = state.manager.get_degrading_speed_trends().await;
    Json(serde_json::to_value(trends).unwrap_or_default())
}

/// GET /api/speed-trend/improving - Get domains with improving trends
async fn get_improving_trends_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let trends = state.manager.get_improving_speed_trends().await;
    Json(serde_json::to_value(trends).unwrap_or_default())
}

/// POST /api/speed-trend/clear - Clear all speed trend data
async fn clear_all_speed_trends_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_all_speed_trends().await;
    Json(serde_json::json!({"status": "ok"}))
}

// --- Task Scorecard (Phase 139) ---

/// GET /api/task-scorecard - Get task scorecard configuration
async fn get_task_scorecard_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_task_scorecard_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/task-scorecard - Update task scorecard configuration
async fn set_task_scorecard_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_scorecard::ScorecardConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_task_scorecard_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/task-scorecard/summary - Get task scorecard summary
async fn get_task_scorecard_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_task_scorecard_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/task-scorecard/list - List all task scorecards
async fn list_task_scorecards_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let scorecards = state.manager.get_all_task_scorecards().await;
    Json(serde_json::to_value(scorecards).unwrap_or_default())
}

/// GET /api/task-scorecard/top - Get top performing tasks
async fn get_top_task_scorecards_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    let n: usize = params.get("n").and_then(|s| s.parse().ok()).unwrap_or(10);
    let top = state.manager.get_top_task_scorecards(n).await;
    Json(serde_json::to_value(top).unwrap_or_default())
}

/// GET /api/task-scorecard/worst - Get worst performing tasks
async fn get_worst_task_scorecards_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    let n: usize = params.get("n").and_then(|s| s.parse().ok()).unwrap_or(10);
    let worst = state.manager.get_worst_task_scorecards(n).await;
    Json(serde_json::to_value(worst).unwrap_or_default())
}

/// POST /api/task-scorecard/generate/:task_id - Generate scorecard for a task
async fn generate_task_scorecard_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.generate_task_scorecard(&task_id).await {
        Some(scorecard) => Json(serde_json::to_value(scorecard).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "Task not found"})),
    }
}

/// GET /api/task-scorecard/:task_id - Get scorecard for a specific task
async fn get_task_scorecard_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_task_scorecard(&task_id).await {
        Some(scorecard) => Json(serde_json::to_value(scorecard).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "Scorecard not found"})),
    }
}

/// DELETE /api/task-scorecard/:task_id - Delete scorecard for a specific task
async fn delete_task_scorecard_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_task_scorecard(&task_id).await;
    Json(serde_json::json!({"status": if removed { "removed" } else { "not_found" }}))
}

/// POST /api/task-scorecard/clear - Clear all task scorecards
async fn clear_all_task_scorecards_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_all_task_scorecards().await;
    Json(serde_json::json!({"status": "ok"}))
}

// --- Event Webhook (Phase 130) ---

/// GET /api/webhook - Get webhook summary
async fn get_webhook_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_webhook_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/webhook/config - Get webhook configuration
async fn get_webhook_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_webhook_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/webhook/config - Update webhook configuration
async fn set_webhook_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::event_webhook::WebhookConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_webhook_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/webhook/endpoints - List all webhook endpoints
async fn list_webhook_endpoints_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let endpoints = state.manager.list_webhook_endpoints().await;
    Json(serde_json::to_value(endpoints).unwrap_or_default())
}

/// POST /api/webhook/endpoints - Add a new webhook endpoint
async fn add_webhook_endpoint_handler(
    State(state): State<Arc<WebState>>,
    Json(endpoint): Json<crate::event_webhook::WebhookEndpoint>,
) -> impl axum::response::IntoResponse {
    match state.manager.add_webhook_endpoint(endpoint).await {
        Ok(id) => (StatusCode::CREATED, Json(serde_json::json!({"id": id}))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/webhook/endpoints/:id - Get a specific webhook endpoint
async fn get_webhook_endpoint_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_webhook_endpoint(&id).await {
        Some(endpoint) => Json(serde_json::to_value(endpoint).unwrap_or_default()).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "endpoint not found"})),
        )
            .into_response(),
    }
}

/// PUT /api/webhook/endpoints/:id - Update a webhook endpoint
async fn update_webhook_endpoint_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    Json(updates): Json<WebhookEndpointUpdateRequest>,
) -> impl axum::response::IntoResponse {
    let update = crate::event_webhook::WebhookEndpointUpdate {
        url: updates.url,
        name: updates.name,
        enabled: updates.enabled,
        secret: updates.secret,
        timeout_secs: updates.timeout_secs,
        max_retries: updates.max_retries,
        events: updates.events,
        headers: updates.headers,
    };
    match state.manager.update_webhook_endpoint(&id, update).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/webhook/endpoints/:id - Remove a webhook endpoint
async fn remove_webhook_endpoint_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.remove_webhook_endpoint(&id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/webhook/endpoints/:id/history - Get delivery history for an endpoint
async fn get_webhook_history_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> impl axum::response::IntoResponse {
    let history = state.manager.get_webhook_history(&id, 50).await;
    Json(serde_json::to_value(history).unwrap_or_default())
}

/// POST /api/webhook/endpoints/:id/history - Clear delivery history for an endpoint
async fn clear_webhook_history_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.clear_webhook_history(&id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/webhook/history - Clear all webhook delivery history
async fn clear_all_webhook_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_all_webhook_history().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// Request body for updating webhook endpoint
#[derive(Debug, Deserialize)]
struct WebhookEndpointUpdateRequest {
    url: Option<String>,
    name: Option<String>,
    enabled: Option<bool>,
    secret: Option<String>,
    timeout_secs: Option<u64>,
    max_retries: Option<u32>,
    events: Option<Vec<crate::event_webhook::WebhookEvent>>,
    headers: Option<std::collections::HashMap<String, String>>,
}

// ===== Path Organizer API Handlers =====

/// GET /api/path-organizer - Get path organizer configuration
async fn get_path_organizer_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_path_organizer_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/path-organizer - Set path organizer configuration
async fn set_path_organizer_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::path_organizer::PathOrganizerConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_path_organizer_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/path-organizer/summary - Get path organizer summary
async fn get_path_organizer_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_path_organizer_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// POST /api/path-organizer/reset - Reset path organizer summary
async fn reset_path_organizer_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.reset_path_organizer_summary().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/path-organizer/categories - List file categories
async fn list_file_categories_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let categories = state.manager.list_file_categories().await;
    Json(serde_json::to_value(categories).unwrap_or_default())
}

/// POST /api/path-organizer/categories - Add a file category
async fn add_file_category_handler(
    State(state): State<Arc<WebState>>,
    Json(category): Json<crate::path_organizer::FileCategory>,
) -> impl axum::response::IntoResponse {
    state.manager.add_file_category(category).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// DELETE /api/path-organizer/categories/:name - Remove a file category
async fn remove_file_category_handler(
    State(state): State<Arc<WebState>>,
    Path(name): Path<String>,
) -> impl axum::response::IntoResponse {
    if state.manager.remove_file_category(&name).await {
        Json(serde_json::json!({"status": "ok"})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "category not found"})),
        )
            .into_response()
    }
}

/// POST /api/path-organizer/organize/:task_id - Organize a task's file
async fn organize_task_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> impl axum::response::IntoResponse {
    let tasks = state.manager.list_tasks().await;
    let task = tasks.iter().find(|t| t.id == task_id);
    match task {
        Some(t) => match state.manager.organize_completed_file(&t.id).await {
            Ok(Some(result)) => {
                Json(serde_json::to_value(result).unwrap_or_default()).into_response()
            }
            Ok(None) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "file not found or already organized"})),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "task not found"})),
        )
            .into_response(),
    }
}

// ===== Upload Tracker API Handlers =====

/// GET /api/upload-tracker - Get upload tracker configuration
async fn get_upload_tracker_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_upload_tracker_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/upload-tracker - Set upload tracker configuration
async fn set_upload_tracker_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::upload_tracker::UploadTrackerConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_upload_tracker_config(config).await;
    StatusCode::OK
}

/// GET /api/upload-tracker/summary - Get upload tracker summary
async fn get_upload_tracker_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_upload_tracker_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// POST /api/upload-tracker/clear - Clear all upload tracking data
async fn clear_upload_tracker_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_upload_tracking().await;
    StatusCode::OK
}

/// GET /api/upload-tracker/tasks - List all tracked task IDs
async fn list_upload_tracked_tasks_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let tasks = state.manager.list_upload_tracked_tasks().await;
    Json(tasks)
}

// ===== Data Retention API Handlers =====

/// GET /api/data-retention - Get data retention configuration
async fn get_data_retention_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_data_retention_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/data-retention - Set data retention configuration
async fn set_data_retention_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::data_retention::DataRetentionConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_data_retention_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/data-retention/summary - Get data retention summary
async fn get_data_retention_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_data_retention_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/data-retention/rules - List retention rules
async fn list_data_retention_rules_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let rules = state.manager.list_data_retention_rules().await;
    Json(serde_json::to_value(rules).unwrap_or_default())
}

/// POST /api/data-retention/rules - Add a retention rule
async fn add_data_retention_rule_handler(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::data_retention::RetentionRule>,
) -> impl axum::response::IntoResponse {
    match state.manager.add_data_retention_rule(rule).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/data-retention/rules/:id - Remove a retention rule
async fn remove_data_retention_rule_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> impl axum::response::IntoResponse {
    if state.manager.remove_data_retention_rule(&id).await {
        Json(serde_json::json!({"status": "ok"})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "rule not found"})),
        )
            .into_response()
    }
}

/// POST /api/data-retention/cleanup - Execute retention cleanup
async fn execute_data_retention_cleanup_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let reason = match body.get("reason").and_then(|v| v.as_str()) {
        Some("manual") => crate::data_retention::CleanupReason::Manual,
        Some("retention_expired") => crate::data_retention::CleanupReason::RetentionExpired,
        Some("disk_pressure") => crate::data_retention::CleanupReason::DiskPressure,
        Some("size_limit") => crate::data_retention::CleanupReason::SizeLimitExceeded,
        Some(r) => crate::data_retention::CleanupReason::RuleBased(r.to_string()),
        None => crate::data_retention::CleanupReason::Manual,
    };

    match state.manager.execute_retention_cleanup(reason).await {
        Ok(result) => Json(serde_json::to_value(result).unwrap_or_default()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/data-retention/history - Get cleanup history
async fn get_data_retention_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let history = state.manager.get_data_retention_history().await;
    Json(serde_json::to_value(history).unwrap_or_default())
}

/// POST /api/data-retention/history/clear - Clear cleanup history
async fn clear_data_retention_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_data_retention_history().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ===== Source Quality API Handlers =====

/// GET /api/source-quality - Get source quality configuration
async fn get_source_quality_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_source_quality_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/source-quality - Set source quality configuration
async fn set_source_quality_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::source_quality::SourceQualityConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_source_quality_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/source-quality/summary - Get source quality summary
async fn get_source_quality_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_source_quality_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/source-quality/:source_id - Get quality details for a specific source
async fn get_source_quality_detail_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(source_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    match state.manager.get_source_quality(&source_id).await {
        Some(detail) => Json(serde_json::to_value(detail).unwrap_or_default()).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "source not found"})),
        )
            .into_response(),
    }
}

/// POST /api/source-quality/:source_id/unblock - Unblock a source
async fn unblock_source_quality_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(source_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let ok = state.manager.unblock_source_quality(&source_id).await;
    if ok {
        Json(serde_json::json!({"status": "ok"})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "source not found"})),
        )
            .into_response()
    }
}

/// DELETE /api/source-quality/:source_id - Remove a source from tracking
async fn remove_source_quality_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(source_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let ok = state.manager.remove_source_quality(&source_id).await;
    if ok {
        Json(serde_json::json!({"status": "ok"})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "source not found"})),
        )
            .into_response()
    }
}

/// POST /api/source-quality/recommend - Recommend best source from candidates
async fn recommend_source_quality_handler(
    State(state): State<Arc<WebState>>,
    Json(candidates): Json<Vec<String>>,
) -> impl axum::response::IntoResponse {
    match state.manager.recommend_source_quality(&candidates).await {
        Some(recommended) => Json(serde_json::json!({"recommended": recommended})).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no suitable source found"})),
        )
            .into_response(),
    }
}

/// POST /api/source-quality/clear - Clear all source quality data
async fn clear_source_quality_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_source_quality().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ============================================================================
// Phase 136: Bandwidth Forecast CLI + REST API Integration
// ============================================================================

/// GET /api/bandwidth-forecast - Get bandwidth forecast configuration
async fn get_bandwidth_forecast_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_bandwidth_forecast_config().await;
    Json(serde_json::json!({
        "config": config,
        "enabled": config.enabled,
        "min_samples": config.min_samples,
        "max_samples": config.max_samples,
        "trend_window_secs": config.trend_window_secs,
        "high_confidence_threshold": config.high_confidence_threshold,
        "medium_confidence_threshold": config.medium_confidence_threshold
    }))
}

/// POST /api/bandwidth-forecast - Set bandwidth forecast configuration
async fn set_bandwidth_forecast_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::bandwidth_forecast::ForecastConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_bandwidth_forecast_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/bandwidth-forecast/summary - Get forecast summary for all domains
async fn get_bandwidth_forecast_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_bandwidth_forecast_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/bandwidth-forecast/predict/:domain - Predict bandwidth for a specific domain
async fn predict_bandwidth_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let forecast = state.manager.forecast_bandwidth(&domain).await;
    Json(serde_json::to_value(forecast).unwrap_or_default())
}

/// DELETE /api/bandwidth-forecast/domain/:domain - Remove forecast data for a domain
async fn remove_bandwidth_forecast_domain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_bandwidth_forecast_domain(&domain).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/bandwidth-forecast/clear - Clear all bandwidth forecast data
async fn clear_bandwidth_forecast_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_bandwidth_forecast().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ─────────────────────────────────────────────────────────────
// Phase 140: Intelligent Source Selector REST API Handlers
// ─────────────────────────────────────────────────────────────

/// GET /api/intelligent-selector - Get intelligent selector config
async fn get_intelligent_selector_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_intelligent_selector_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/intelligent-selector - Update intelligent selector config
async fn set_intelligent_selector_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::intelligent_source_selector::IntelligentSelectorConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_intelligent_selector_config(config).await;
    StatusCode::OK
}

/// GET /api/intelligent-selector/summary - Get selector summary
async fn get_intelligent_selector_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_intelligent_selector_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// POST /api/intelligent-selector/select/:task_id - Select sources for a task
async fn select_intelligent_sources_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let result = state.manager.select_intelligent_sources(&task_id).await;
    Json(serde_json::to_value(result).unwrap_or_default())
}

/// GET /api/intelligent-selector/candidates/:task_id - Get candidates for a task
async fn get_intelligent_selector_candidates_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let candidates = state
        .manager
        .get_intelligent_source_candidates(&task_id)
        .await;
    Json(serde_json::to_value(candidates).unwrap_or_default())
}

/// GET /api/intelligent-selector/history - Get selection history
async fn get_intelligent_selector_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let history = state.manager.get_intelligent_selector_history().await;
    Json(serde_json::to_value(history).unwrap_or_default())
}

/// POST /api/intelligent-selector/history/clear - Clear selection history
async fn clear_intelligent_selector_history_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_intelligent_selector_history().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/intelligent-selector/clear - Clear all data
async fn clear_intelligent_selector_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_intelligent_selector().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ─────────────────────────────────────────────────────────────
// Phase 138: Speed Boost REST API Handlers
// ─────────────────────────────────────────────────────────────

/// GET /api/speed-boost - Get speed boost status and config
async fn get_speed_boost_status_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let status = state.manager.get_speed_boost_status().await;
    Json(serde_json::to_value(status).unwrap_or_default())
}

/// POST /api/speed-boost - Update speed boost configuration
async fn set_speed_boost_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_boost::SpeedBoostConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_speed_boost_config(config).await;
    StatusCode::OK
}

/// POST /api/speed-boost/start - Start a speed boost
async fn start_speed_boost_handler(
    State(state): State<Arc<WebState>>,
    Json(params): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let duration_secs = params.get("duration_secs").and_then(|v| v.as_u64());
    let multiplier = params.get("multiplier").and_then(|v| v.as_f64());
    let preset = params
        .get("preset")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let result = if let Some(preset_name) = preset {
        state.manager.start_speed_boost_preset(&preset_name).await
    } else {
        state
            .manager
            .start_speed_boost(duration_secs, multiplier)
            .await
    };

    match result {
        crate::speed_boost::BoostStartResult::Started(boost) => Json(serde_json::json!({
            "status": "started",
            "multiplier": boost.multiplier,
            "expires_at": boost.expires_at,
            "source": boost.source,
            "remaining_secs": boost.remaining_secs()
        })),
        crate::speed_boost::BoostStartResult::Disabled => Json(
            serde_json::json!({"status": "disabled", "message": "Speed boost feature is disabled"}),
        ),
        crate::speed_boost::BoostStartResult::AlreadyActive => Json(
            serde_json::json!({"status": "already_active", "message": "Another boost is already active"}),
        ),
        crate::speed_boost::BoostStartResult::InvalidParams(msg) => {
            Json(serde_json::json!({"status": "invalid_params", "message": msg}))
        }
    }
}

/// POST /api/speed-boost/stop - Stop the active speed boost
async fn stop_speed_boost_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let stopped = state.manager.stop_speed_boost().await;
    Json(serde_json::json!({"status": if stopped { "stopped" } else { "no_active_boost" }}))
}

/// POST /api/speed-boost/preset - Add a named boost preset
async fn add_speed_boost_preset_handler(
    State(state): State<Arc<WebState>>,
    Json(preset_req): Json<AddPresetRequest>,
) -> impl axum::response::IntoResponse {
    let preset = crate::speed_boost::BoostPreset {
        name: preset_req.name.clone(),
        multiplier: preset_req.multiplier,
        duration_secs: preset_req.duration_secs,
        description: preset_req.description.unwrap_or_default(),
    };
    let added = state
        .manager
        .add_speed_boost_preset(&preset_req.id, preset)
        .await;
    Json(serde_json::json!({"status": if added { "added" } else { "failed" }}))
}

/// DELETE /api/speed-boost/preset/:id - Remove a named boost preset
async fn remove_speed_boost_preset_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_speed_boost_preset(&id).await;
    Json(serde_json::json!({"status": if removed { "removed" } else { "not_found" }}))
}

/// GET /api/speed-boost/presets - List all named boost presets
async fn list_speed_boost_presets_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_speed_boost_config().await;
    Json(serde_json::to_value(config.presets).unwrap_or_default())
}

/// GET /api/speed-boost/scheduled - List all scheduled boost windows
async fn list_speed_boost_scheduled_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_speed_boost_config().await;
    Json(serde_json::to_value(config.scheduled_windows).unwrap_or_default())
}

/// POST /api/speed-boost/scheduled - Add a scheduled boost window
async fn add_speed_boost_scheduled_handler(
    State(state): State<Arc<WebState>>,
    Json(window): Json<crate::speed_boost::ScheduledBoostWindow>,
) -> impl axum::response::IntoResponse {
    let added = state.manager.add_scheduled_boost_window(window).await;
    Json(serde_json::json!({"status": if added { "added" } else { "failed" }}))
}

/// DELETE /api/speed-boost/scheduled/:id - Remove a scheduled boost window
async fn remove_speed_boost_scheduled_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let removed = state.manager.remove_scheduled_boost_window(&id).await;
    Json(serde_json::json!({"status": if removed { "removed" } else { "not_found" }}))
}

/// Request body for adding a boost preset
#[derive(Debug, serde::Deserialize)]
struct AddPresetRequest {
    id: String,
    name: String,
    multiplier: f64,
    duration_secs: u64,
    #[serde(default)]
    description: Option<String>,
}

// ─── Phase 142: Retry Budget API Handlers ───

/// GET /api/retry-budget - Get retry budget configuration
async fn get_retry_budget_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_retry_budget_config().await;
    Json(config)
}

/// POST /api/retry-budget - Set retry budget configuration
async fn set_retry_budget_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::retry_budget::RetryBudgetConfig>,
) -> impl axum::response::IntoResponse {
    state.manager.set_retry_budget_config(config).await;
    let _ = state.manager.save_retry_budget_config().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/retry-budget/summary - Get retry budget summary
async fn get_retry_budget_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_retry_budget_summary().await;
    Json(summary)
}

/// GET /api/retry-budget/check/:domain - Check if domain can be retried
async fn check_retry_budget_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    let can_retry = state.manager.can_retry_domain(&domain).await;
    let remaining = state.manager.get_remaining_retry_budget(&domain).await;
    let state_info = state.manager.get_domain_retry_state(&domain).await;
    Json(serde_json::json!({
        "domain": domain,
        "can_retry": can_retry,
        "remaining_budget": remaining,
        "state": state_info
    }))
}

/// POST /api/retry-budget/record-retry/:domain - Record a retry attempt
async fn record_retry_budget_retry_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    state.manager.record_retry_domain(&domain).await;
    let remaining = state.manager.get_remaining_retry_budget(&domain).await;
    Json(serde_json::json!({"status": "recorded", "remaining_budget": remaining}))
}

/// POST /api/retry-budget/record-success/:domain - Record a successful download
async fn record_retry_budget_success_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    state.manager.record_success_domain(&domain).await;
    Json(serde_json::json!({"status": "recorded"}))
}

/// POST /api/retry-budget/clear/:domain - Clear retry state for a domain
async fn clear_domain_retry_budget_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_domain_retry_state(&domain).await;
    Json(serde_json::json!({"status": "cleared"}))
}

/// POST /api/retry-budget/clear - Clear all retry budget state
async fn clear_all_retry_budget_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    state.manager.clear_all_retry_budget_state().await;
    Json(serde_json::json!({"status": "cleared"}))
}

/// GET /api/uptime - Get system uptime information
async fn get_uptime_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_uptime_summary().await;
    Json(serde_json::json!({
        "uptime_seconds": summary.uptime_seconds,
        "uptime_formatted": summary.uptime_formatted,
        "started_at": summary.started_at
    }))
}

// ── File Type Statistics Handlers (Phase 143) ─────────────────────────

/// GET /api/file-stats - Get file statistics configuration
async fn get_file_stats_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_file_stats_config().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "max_extensions": config.max_extensions,
        "track_extensions": config.track_extensions,
        "track_categories": config.track_categories
    }))
}

/// POST /api/file-stats - Update file statistics configuration
async fn set_file_stats_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_file_stats::FileStatsConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_file_stats_config(config).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// GET /api/file-stats/summary - Get file statistics summary
async fn get_file_stats_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_file_stats_summary().await;
    Json(summary)
}

/// POST /api/file-stats/clear - Clear all file statistics
async fn clear_file_stats_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    match state.manager.clear_file_stats().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// GET /api/file-stats/extension/:ext - Get stats for a specific extension
async fn get_extension_stats_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(ext): axum::extract::Path<String>,
) -> Result<Json<crate::download_file_stats::ExtensionStats>, StatusCode> {
    state
        .manager
        .get_extension_file_stats(&ext)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ========== Phase 144: SLA Compliance REST API Handlers ==========

/// GET /api/sla-compliance - Get SLA compliance configuration
async fn get_sla_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<sla_compliance::SlaConfig> {
    let config = state.manager.get_sla_config().await;
    Json(config)
}

/// POST /api/sla-compliance - Update SLA compliance configuration
async fn set_sla_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<sla_compliance::SlaConfig>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .set_sla_config(config)
        .await
        .map(|_| Json(serde_json::json!({"status": "ok"})))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// GET /api/sla-compliance/summary - Get SLA compliance summary
async fn get_sla_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<sla_compliance::SlaSummary> {
    let summary = state.manager.get_sla_summary().await;
    Json(summary)
}

/// GET /api/sla-compliance/definitions - List all SLA definitions
async fn list_sla_definitions_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<sla_compliance::SlaDefinition>> {
    let slas = state.manager.list_slas().await;
    Json(slas)
}

/// POST /api/sla-compliance/definitions - Add a new SLA definition
async fn add_sla_definition_handler(
    State(state): State<Arc<WebState>>,
    Json(definition): Json<sla_compliance::SlaDefinition>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .add_sla(definition)
        .await
        .map(|id| Json(serde_json::json!({"status": "ok", "id": id})))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// GET /api/sla-compliance/definitions/:id - Get a specific SLA definition
async fn get_sla_definition_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Result<Json<sla_compliance::SlaDefinition>, StatusCode> {
    state
        .manager
        .get_sla(&id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// DELETE /api/sla-compliance/definitions/:id - Delete an SLA definition
async fn delete_sla_definition_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let removed = state.manager.remove_sla(&id).await;
    removed
        .map(|_| Json(serde_json::json!({"status": "ok"})))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// POST /api/sla-compliance/definitions/:id/enable - Enable or disable an SLA
async fn set_sla_enabled_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let enabled = body["enabled"].as_bool().unwrap_or(true);
    let result = state.manager.set_sla_enabled(&id, enabled).await;
    result
        .map(|_| Json(serde_json::json!({"status": "ok"})))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// POST /api/sla-compliance/evaluate - Evaluate all enabled SLAs
async fn evaluate_sla_compliance_handler(
    State(state): State<Arc<WebState>>,
) -> Result<Json<Vec<sla_compliance::SlaEvaluation>>, StatusCode> {
    state
        .manager
        .evaluate_sla_compliance()
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// GET /api/sla-compliance/history/:id - Get compliance history for an SLA
async fn get_sla_history_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<sla_compliance::ComplianceEntry>>, StatusCode> {
    state
        .manager
        .get_sla_history(&id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// POST /api/sla-compliance/history/:id/clear - Clear compliance history for an SLA
async fn clear_sla_history_handler(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .clear_sla_history(&id)
        .await
        .map(|_| Json(serde_json::json!({"status": "ok"})))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// POST /api/sla-compliance/history/clear - Clear all SLA compliance history
async fn clear_all_sla_history_handler(
    State(state): State<Arc<WebState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .clear_all_sla_history()
        .await
        .map(|_| Json(serde_json::json!({"status": "ok"})))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// GET /api/sla-compliance/report - Get human-readable SLA compliance report
async fn get_sla_report_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let report = state.manager.format_sla_report().await;
    Json(serde_json::json!({"report": report}))
}

// ========== Speed Alert API (Phase 167) ==========

/// GET /api/speed-alerts - Get speed alert configuration
async fn get_speed_alert_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_alert::SpeedAlertConfig> {
    Json(state.manager.get_speed_alert_config().await)
}

/// POST /api/speed-alerts - Update speed alert configuration
async fn set_speed_alert_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_alert::SpeedAlertConfig>,
) -> StatusCode {
    state.manager.set_speed_alert_config(config).await;
    StatusCode::OK
}

/// GET /api/speed-alerts/summary - Get speed alert summary
async fn get_speed_alert_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_alert::SpeedAlertSummary> {
    Json(state.manager.get_speed_alert_summary().await)
}

/// GET /api/speed-alerts/history - Get speed alert history
async fn get_speed_alert_history_handler(
    State(state): State<Arc<WebState>>,
    Query(params): Query<SpeedAlertHistoryParams>,
) -> Json<Vec<crate::speed_alert::SpeedAlert>> {
    let limit = params.limit.unwrap_or(50).min(200);
    Json(state.manager.get_speed_alerts(limit).await)
}

/// POST /api/speed-alerts/history/clear - Clear all speed alert history
async fn clear_speed_alert_history_handler(State(state): State<Arc<WebState>>) -> StatusCode {
    state.manager.clear_speed_alert_history().await;
    StatusCode::OK
}

/// GET /api/speed-alerts/task/:task_id - Get alerts for a specific task
async fn get_task_speed_alerts_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
    Query(params): Query<SpeedAlertHistoryParams>,
) -> Json<Vec<crate::speed_alert::SpeedAlert>> {
    let limit = params.limit.unwrap_or(50).min(200);
    Json(state.manager.get_task_speed_alerts(&task_id, limit).await)
}

/// POST /api/speed-alerts/task/:task_id/remove - Remove a task from speed alert monitoring
async fn remove_speed_alert_task_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> StatusCode {
    state.manager.remove_speed_alert_task(&task_id).await;
    StatusCode::OK
}

/// POST /api/speed-alerts/enable - Enable or disable speed alerts
async fn set_speed_alert_enabled_handler(
    State(state): State<Arc<WebState>>,
    Json(params): Json<SpeedAlertEnabledParams>,
) -> StatusCode {
    state.manager.set_speed_alert_enabled(params.enabled).await;
    StatusCode::OK
}

/// POST /api/speed-alerts/monitors/clear - Clear all speed alert monitoring state
async fn clear_speed_alert_monitors_handler(State(state): State<Arc<WebState>>) -> StatusCode {
    state.manager.clear_speed_alert_monitors().await;
    StatusCode::OK
}

#[derive(Deserialize)]
struct SpeedAlertHistoryParams {
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SpeedAlertEnabledParams {
    enabled: bool,
}

// ========== Download Session API (Phase 165) ==========

/// GET /api/download-session - Get all session summaries
async fn get_all_session_summaries_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::download_session::TaskSessionSummary>> {
    let summaries = state.manager.get_all_session_summaries().await;
    Json(summaries)
}

/// GET /api/download-session/config - Get download session configuration
async fn get_download_session_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::download_session::DownloadSessionConfig> {
    let config = state.manager.get_download_session_config().await;
    Json(config)
}

/// POST /api/download-session/config - Update download session configuration
async fn set_download_session_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_session::DownloadSessionConfig>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state.manager.set_download_session_config(config).await;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// GET /api/download-session/summary - Get download session summary
async fn get_download_session_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summaries = state.manager.get_all_session_summaries().await;
    let total_sessions = summaries.iter().map(|s| s.total_sessions).sum::<usize>();
    let total_bytes = summaries
        .iter()
        .map(|s| s.total_bytes_transferred)
        .sum::<u64>();
    let total_duration = summaries
        .iter()
        .map(|s| s.total_download_time_secs)
        .sum::<f64>();
    let avg_speed = if total_duration > 0.0 {
        total_bytes as f64 / total_duration
    } else {
        0.0
    };
    Json(serde_json::json!({
        "total_tasks": summaries.len(),
        "total_sessions": total_sessions,
        "total_bytes_transferred": total_bytes,
        "total_duration_secs": total_duration,
        "average_speed_bps": avg_speed
    }))
}

/// GET /api/download-session/task/:task_id - Get task session summary
async fn get_task_session_summary_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::download_session::TaskSessionSummary>, StatusCode> {
    match state.manager.get_task_session_summary(&task_id).await {
        Some(summary) => Ok(Json(summary)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// DELETE /api/download-session/task/:task_id - Remove task sessions
async fn remove_task_sessions_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let removed = state.manager.remove_task_sessions(&task_id).await;
    if removed {
        Ok(Json(serde_json::json!({"status": "ok", "removed": true})))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// POST /api/download-session/clear - Clear all sessions
async fn clear_all_sessions_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_all_sessions().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ========== Connection Pool API (Phase 164) ==========

/// GET /api/connection-pool - Get connection pool status (detailed)
async fn get_connection_pool_status_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::connection_pool::PoolStatus> {
    let status = state.manager.get_connection_pool_status().await;
    Json(status)
}

/// POST /api/connection-pool - Update connection pool configuration
async fn set_connection_pool_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::connection_pool::PoolConfig>,
) -> StatusCode {
    state.manager.set_connection_pool_config(config).await;
    StatusCode::OK
}

/// GET /api/connection-pool/stats - Get connection pool statistics
async fn get_connection_pool_stats_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::connection_pool::PoolStats> {
    let stats = state.manager.get_connection_pool_stats().await;
    Json(stats)
}

/// GET /api/connection-pool/config - Get connection pool configuration
async fn get_connection_pool_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::connection_pool::PoolConfig> {
    let config = state.manager.get_connection_pool_config().await;
    Json(config)
}

/// GET /api/connection-pool/domains - Get per-domain connection information
async fn get_connection_pool_domains_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::connection_pool::DomainConnectionInfo>> {
    let domains = state.manager.get_connection_pool_domains().await;
    Json(domains)
}

/// POST /api/connection-pool/domain/:domain - Set per-domain connection limit
async fn set_connection_pool_domain_limit_handler(
    State(state): State<Arc<WebState>>,
    Path(domain): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    let limit = body.get("limit").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    state
        .manager
        .set_connection_pool_domain_limit(&domain, limit)
        .await;
    StatusCode::OK
}

/// POST /api/connection-pool/cleanup - Remove expired connections
async fn cleanup_connection_pool_handler(State(state): State<Arc<WebState>>) -> StatusCode {
    state.manager.cleanup_connection_pool().await;
    StatusCode::OK
}

/// POST /api/connection-pool/clear - Clear all connections and reset statistics
async fn clear_connection_pool_handler(State(state): State<Arc<WebState>>) -> StatusCode {
    state.manager.clear_connection_pool().await;
    StatusCode::OK
}

/// POST /api/connection-pool/save - Save connection pool configuration to disk
async fn save_connection_pool_config_handler(State(state): State<Arc<WebState>>) -> StatusCode {
    match state.manager.save_connection_pool_config().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ========== Source Reliability Tracker API (Phase 163) ==========

/// GET /api/source-reliability - Get source reliability configuration
async fn get_source_reliability_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_source_reliability_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/source-reliability - Update source reliability configuration
async fn set_source_reliability_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::source_reliability::SourceReliabilityConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_source_reliability_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/source-reliability/summary - Get source reliability summary
async fn get_source_reliability_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_source_reliability_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/source-reliability/report - Get formatted reliability report
async fn get_source_reliability_report_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let report = state.manager.format_source_reliability_summary().await;
    Json(serde_json::json!({"report": report}))
}

/// GET /api/source-reliability/domains - List all tracked domains
async fn list_source_reliability_domains_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let domains = state.manager.get_source_reliability_domains().await;
    Json(serde_json::to_value(domains).unwrap_or_default())
}

/// GET /api/source-reliability/domain/:domain - Get reliability data for a domain
async fn get_source_reliability_domain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.get_source_reliability_domain(&domain).await {
        Some(dr) => Ok(Json(serde_json::to_value(dr).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// DELETE /api/source-reliability/domain/:domain - Clear reliability data for a domain
async fn clear_source_reliability_domain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> StatusCode {
    state.manager.clear_source_reliability_domain(&domain).await;
    StatusCode::OK
}

/// GET /api/source-reliability/score/:domain - Get reliability score for a domain
async fn get_source_reliability_score_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let score = state.manager.get_source_reliability_score(&domain).await;
    let tier = state.manager.get_source_reliability_tier(&domain).await;
    Json(serde_json::json!({"domain": domain, "score": score, "tier": format!("{}", tier)}))
}

/// GET /api/source-reliability/avoid - Get domains marked for avoidance
async fn get_source_reliability_avoid_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let avoid = state.manager.get_source_reliability_avoid().await;
    let avoid_json: Vec<serde_json::Value> = avoid
        .into_iter()
        .map(|(domain, score)| serde_json::json!({"domain": domain, "score": score}))
        .collect();
    Json(serde_json::json!({"avoid_domains": avoid_json}))
}

/// POST /api/source-reliability/prune - Prune old samples
async fn prune_source_reliability_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let timestamp = body
        .get("before_timestamp")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    state
        .manager
        .prune_source_reliability_samples(timestamp)
        .await;
    Json(serde_json::json!({"status": "ok", "pruned_before": timestamp}))
}

/// POST /api/source-reliability/clear - Clear all reliability data
async fn clear_source_reliability_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_source_reliability().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ========== Notification Center API (Phase 147) ==========

/// GET /api/notification-center - Get notification center configuration
async fn get_notification_center_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_notification_center_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/notification-center - Update notification center configuration
async fn set_notification_center_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::notification_center::NotificationCenterConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_notification_center_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/notification-center/summary - Get notification center summary
async fn get_notification_center_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_notification_center_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/notification-center/history - Get notification history with filters
async fn get_notification_center_history_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let filter = crate::notification_center::NotificationFilter {
        event: params
            .get("event")
            .and_then(|e| serde_json::from_str(&format!("\"{}\"", e)).ok()),
        min_priority: params
            .get("min_priority")
            .and_then(|p| serde_json::from_str(&format!("\"{}\"", p)).ok()),
        channel: params.get("channel").cloned(),
        task_id: params.get("task_id").cloned(),
        suppressed: params.get("suppressed").and_then(|s| s.parse().ok()),
        limit: params.get("limit").and_then(|l| l.parse().ok()),
        ..Default::default()
    };
    let history = state.manager.get_notification_history(filter).await;
    Json(serde_json::json!({
        "entries": history,
        "count": history.len()
    }))
}

/// POST /api/notification-center/history/clear - Clear notification history
async fn clear_notification_center_history_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_notification_history().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/notification-center/analytics - Get notification analytics
async fn get_notification_center_analytics_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let analytics = state.manager.get_notification_analytics().await;
    Json(serde_json::to_value(analytics).unwrap_or_default())
}

/// POST /api/notification-center/flush - Flush pending notification batch
async fn flush_notification_batch_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.flush_notification_batch().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/notification-center/event-prefs - Add event channel preference
async fn add_event_preference_handler(
    State(state): State<Arc<WebState>>,
    Json(preference): Json<crate::notification_center::EventChannelPreference>,
) -> Json<serde_json::Value> {
    state
        .manager
        .add_notification_event_preference(preference)
        .await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/notification-center/event-prefs/remove - Remove event channel preference
async fn remove_event_preference_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let event_str = params.get("event").ok_or(StatusCode::BAD_REQUEST)?;
    let event: crate::notification_center::NotificationCenterEvent =
        serde_json::from_str(&format!("\"{}\"", event_str)).map_err(|_| StatusCode::BAD_REQUEST)?;
    state
        .manager
        .remove_notification_event_preference(event)
        .await;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

// ========== Notification Preferences API (Phase 161) ==========

/// GET /api/notification-preferences - Get notification preferences configuration
async fn get_notification_preferences_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_notification_preferences_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/notification-preferences - Update notification preferences configuration
async fn set_notification_preferences_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::notification_preferences::NotificationPreferencesConfig>,
) -> Json<serde_json::Value> {
    state
        .manager
        .set_notification_preferences_config(config)
        .await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/notification-preferences/summary - Get notification preferences summary
async fn get_notification_preferences_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_notification_preferences_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/notification-preferences/tasks - List all task notification configs
async fn list_notification_preferences_tasks_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let tasks = state.manager.list_task_notification_configs().await;
    Json(serde_json::to_value(tasks).unwrap_or_default())
}

/// GET /api/notification-preferences/task/:task_id - Get task notification config
async fn get_task_notification_preferences_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.get_task_notification_config(&task_id).await {
        Some(config) => Ok(Json(serde_json::to_value(config).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/notification-preferences/task/:task_id - Set task notification config
async fn set_task_notification_preferences_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
    Json(mut config): Json<crate::notification_preferences::TaskNotificationConfig>,
) -> Json<serde_json::Value> {
    config.task_id = task_id;
    state.manager.set_task_notification_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// DELETE /api/notification-preferences/task/:task_id - Remove task notification config
async fn remove_task_notification_preferences_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    let removed = state
        .manager
        .remove_task_notification_config(&task_id)
        .await;
    Json(serde_json::json!({"removed": removed}))
}

/// POST /api/notification-preferences/task/:task_id/enable - Enable notifications for task
async fn enable_task_notifications_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    state.manager.enable_task_notifications(&task_id).await;
    Json(serde_json::json!({"status": "enabled"}))
}

/// POST /api/notification-preferences/task/:task_id/disable - Disable notifications for task
async fn disable_task_notifications_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    state.manager.disable_task_notifications(&task_id).await;
    Json(serde_json::json!({"status": "disabled"}))
}

/// POST /api/notification-preferences/cooldown/clear - Clear all notification cooldowns
async fn clear_notification_cooldowns_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_all_notification_cooldowns().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/notification-preferences/cooldown/clear/:task_id - Clear task cooldown
async fn clear_task_notification_cooldown_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    state
        .manager
        .clear_task_notification_cooldown(&task_id)
        .await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/notification-preferences/check - Check if notification should be sent
async fn check_notification_handler(
    State(state): State<Arc<WebState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let task_id = payload
        .get("task_id")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let event_str = payload
        .get("event")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let event: crate::notification_preferences::TaskNotificationEvent =
        serde_json::from_str(&format!("\"{}\"", event_str)).map_err(|_| StatusCode::BAD_REQUEST)?;
    let should_send = state
        .manager
        .should_send_notification(task_id, &event)
        .await;
    Ok(Json(serde_json::json!({"should_send": should_send})))
}

// ========== Download Expiry API (Phase 148) ==========

/// GET /api/expiry - Get expiry configuration
async fn get_expiry_config_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_expiry_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/expiry - Update expiry configuration
async fn set_expiry_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_expiry::ExpiryConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_expiry_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/expiry/summary - Get expiry summary
async fn get_expiry_summary_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let summary = state.manager.get_expiry_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// POST /api/expiry/refresh - Refresh all expiry states
async fn refresh_expiries_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let expired = state.manager.refresh_expiries().await;
    Json(serde_json::json!({"expired_count": expired.len(), "expired_ids": expired}))
}

/// POST /api/expiry/clear - Clear all expiry tracking
async fn clear_all_expiries_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_all_expiries().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/expiry/cleanup - Cleanup expired tasks
async fn cleanup_expired_expiries_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let count = state.manager.cleanup_expired_expiries().await;
    Json(serde_json::json!({"cleaned_count": count}))
}

/// GET /api/expiry/report - Get human-readable expiry report
async fn get_expiry_report_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let limit = params
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(20);
    let report = state.manager.format_expiry_report(limit).await;
    Json(serde_json::json!({"report": report}))
}

/// GET /api/expiry/task/:task_id - Get expiry info for a task
async fn get_task_expiry_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.get_task_expiry(&task_id).await {
        Some(expiry) => Ok(Json(serde_json::to_value(expiry).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/expiry/task/:task_id - Set expiry for a task (duration_secs in body)
async fn set_task_expiry_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
    Json(body): Json<SetExpiryRequest>,
) -> Json<serde_json::Value> {
    if let Some(duration_secs) = body.duration_secs {
        state
            .manager
            .set_task_expiry_duration(&task_id, duration_secs)
            .await;
    } else if let Some(expires_at) = body.expires_at {
        let dt = chrono::DateTime::parse_from_rfc3339(&expires_at)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());
        state.manager.set_task_expiry(&task_id, dt).await;
    }
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/expiry/task/:task_id/remove - Remove expiry for a task
async fn remove_task_expiry_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    state.manager.remove_task_expiry(&task_id).await;
    Json(serde_json::json!({"status": "ok"}))
}

#[derive(Debug, Deserialize)]
struct SetExpiryRequest {
    duration_secs: Option<u64>,
    expires_at: Option<String>,
}

// ========== Phase 148: Task Activity REST API Handlers ==========

/// GET /api/task-activity - Get all activity summaries
async fn get_all_activity_summaries_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::task_activity::TaskActivitySummary>> {
    let summaries = state.manager.get_all_activity_summaries().await;
    Json(summaries)
}

/// GET /api/task-activity/:task_id - Get activity log for a specific task
async fn get_task_activity_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::task_activity::TaskActivityLog>, StatusCode> {
    state
        .manager
        .get_task_activity(&task_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// DELETE /api/task-activity/:task_id - Clear activity log for a specific task
async fn clear_task_activity_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    state.manager.clear_task_activity(&task_id).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/task-activity/:task_id/log - Manually log an activity event
#[derive(Debug, Deserialize)]
struct LogActivityRequest {
    event_type: String,
    task_name: String,
    message: Option<String>,
    value: Option<f64>,
}

async fn log_task_activity_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
    Json(req): Json<LogActivityRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let event_type = match req.event_type.as_str() {
        "created" => crate::task_activity::ActivityEventType::Created,
        "started" => crate::task_activity::ActivityEventType::Started,
        "paused" => crate::task_activity::ActivityEventType::Paused,
        "resumed" => crate::task_activity::ActivityEventType::Resumed,
        "completed" => crate::task_activity::ActivityEventType::Completed,
        "failed" => crate::task_activity::ActivityEventType::Failed,
        "removed" => crate::task_activity::ActivityEventType::Removed,
        "auto_retry" => crate::task_activity::ActivityEventType::AutoRetry,
        "speed_limit_changed" => crate::task_activity::ActivityEventType::SpeedLimitChanged,
        "mirror_switched" => crate::task_activity::ActivityEventType::MirrorSwitched,
        "connection_error" => crate::task_activity::ActivityEventType::ConnectionError,
        "timeout" => crate::task_activity::ActivityEventType::Timeout,
        "checksum_verify" => crate::task_activity::ActivityEventType::ChecksumVerify,
        "checksum_result" => crate::task_activity::ActivityEventType::ChecksumResult,
        "hook_executed" => crate::task_activity::ActivityEventType::HookExecuted,
        "cooldown_triggered" => crate::task_activity::ActivityEventType::CooldownTriggered,
        "conflict_resolved" => crate::task_activity::ActivityEventType::ConflictResolved,
        "progress_milestone" => crate::task_activity::ActivityEventType::ProgressMilestone,
        "note_changed" => crate::task_activity::ActivityEventType::NoteChanged,
        "comment_added" => crate::task_activity::ActivityEventType::CommentAdded,
        "tags_changed" => crate::task_activity::ActivityEventType::TagsChanged,
        "info" => crate::task_activity::ActivityEventType::Info,
        "warning" => crate::task_activity::ActivityEventType::Warning,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let message = req
        .message
        .unwrap_or_else(|| format!("Manual log: {}", req.event_type));

    if let Some(value) = req.value {
        state
            .manager
            .log_task_activity_with_value(&task_id, &req.task_name, event_type, message, value)
            .await;
    } else {
        state
            .manager
            .log_task_activity(&task_id, &req.task_name, event_type, message)
            .await;
    }

    Ok(Json(serde_json::json!({"status": "ok"})))
}

// ========== Host Connection Limiter API (Phase 148) ==========

/// GET /api/host-conn-limit - Get host connection limiter configuration
async fn get_host_conn_limit_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_host_conn_limit_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/host-conn-limit - Update host connection limiter configuration
async fn set_host_conn_limit_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::host_conn_limit::HostConnLimitConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_host_conn_limit_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/host-conn-limit/summary - Get host connection limiter summary
async fn get_host_conn_limit_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_host_conn_limit_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/host-conn-limit/host/:hostname - Get connection state for a host
async fn get_host_conn_state_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hostname): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.get_host_connection_state(&hostname).await {
        Some(info) => Ok(Json(serde_json::to_value(info).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/host-conn-limit/host/:hostname/acquire - Acquire a connection slot
async fn acquire_host_connection_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hostname): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let result = state.manager.acquire_host_connection(&hostname).await;
    Json(serde_json::json!({"result": format!("{:?}", result)}))
}

/// POST /api/host-conn-limit/host/:hostname/release - Release a connection slot
async fn release_host_connection_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hostname): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    state.manager.release_host_connection(&hostname).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/host-conn-limit/host/:hostname/failure - Record a connection failure
async fn record_host_failure_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hostname): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    state.manager.record_host_failure(&hostname).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/host-conn-limit/host/:hostname/remove - Remove a host from tracking
async fn remove_host_connection_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hostname): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let removed = state.manager.remove_host_connection(&hostname).await;
    Json(serde_json::json!({"removed": removed}))
}

/// GET /api/host-conn-limit/overrides - List all host overrides
async fn list_host_overrides_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let overrides = state.manager.list_host_connection_overrides().await;
    Json(serde_json::to_value(overrides).unwrap_or_default())
}

/// POST /api/host-conn-limit/overrides - Set a host override
async fn set_host_override_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<SetHostOverrideRequest>,
) -> Json<serde_json::Value> {
    state
        .manager
        .set_host_connection_override(&req.hostname, req.max_connections)
        .await;
    Json(serde_json::json!({"status": "ok"}))
}

/// DELETE /api/host-conn-limit/overrides/:hostname - Remove a host override
async fn remove_host_override_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(hostname): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let removed = state
        .manager
        .remove_host_connection_override(&hostname)
        .await;
    Json(serde_json::json!({"removed": removed}))
}

/// POST /api/host-conn-limit/clear - Clear all host tracking data
async fn clear_host_connections_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_host_connections().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/host-conn-limit/cleanup - Clean up stale host connections
async fn cleanup_stale_hosts_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.cleanup_stale_host_connections().await;
    Json(serde_json::json!({"status": "ok"}))
}

#[derive(serde::Deserialize)]
struct SetHostOverrideRequest {
    hostname: String,
    max_connections: u32,
}

// ── Phase 149: Task Cron Scheduler REST API ─────────────────────────────────

/// GET /api/task-cron-scheduler - Get task cron scheduler configuration
async fn get_task_cron_scheduler_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_task_cron_scheduler_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/task-cron-scheduler - Update task cron scheduler configuration
async fn set_task_cron_scheduler_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::task_cron_scheduler::TaskCronSchedulerConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_task_cron_scheduler_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/task-cron-scheduler/summary - Get task cron scheduler summary
async fn get_task_cron_scheduler_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_task_cron_scheduler_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/task-cron-scheduler/schedules - List all cron schedules
async fn list_task_cron_schedules_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let schedules = state.manager.list_task_cron_schedules().await;
    Json(serde_json::to_value(schedules).unwrap_or_default())
}

/// POST /api/task-cron-scheduler/schedules/:task_id - Add a cron schedule
async fn add_task_cron_schedule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(schedule): Json<crate::task_cron_scheduler::TaskCronSchedule>,
) -> Json<serde_json::Value> {
    match state
        .manager
        .add_task_cron_schedule(&task_id, schedule)
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// DELETE /api/task-cron-scheduler/schedules/:task_id - Remove a cron schedule
async fn remove_task_cron_schedule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.remove_task_cron_schedule(&task_id).await {
        Ok(_) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/task-cron-scheduler/schedules/:task_id/enable - Enable/disable a cron schedule
async fn set_task_cron_schedule_enabled_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(req): Json<SetTaskCronScheduleEnabledRequest>,
) -> Json<serde_json::Value> {
    match state
        .manager
        .set_task_cron_schedule_enabled(&task_id, req.enabled)
        .await
    {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

#[derive(serde::Deserialize)]
struct SetTaskCronScheduleEnabledRequest {
    enabled: bool,
}

// ── Phase 150: Source Latency REST API ──────────────────────────────────────

/// GET /api/source-latency - Get source latency configuration
async fn get_source_latency_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_source_latency_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/source-latency - Update source latency configuration
async fn set_source_latency_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::source_latency::SourceLatencyConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_source_latency_config(config).await;
    let _ = state.manager.save_source_latency_config().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/source-latency/summary - Get source latency summary
async fn get_source_latency_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_source_latency_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/source-latency/domain/:domain - Get latency stats for a specific domain
async fn get_source_latency_domain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.get_source_latency_domain(&domain).await {
        Some(stats) => Json(serde_json::to_value(stats).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "Domain not found"})),
    }
}

/// GET /api/source-latency/all - Get latency stats for all domains
async fn get_source_latency_all_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let all_stats = state.manager.get_source_latency_all().await;
    Json(serde_json::to_value(all_stats).unwrap_or_default())
}

/// GET /api/source-latency/best - Get the best domain (lowest latency)
async fn get_best_latency_domain_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    match state.manager.get_best_latency_domain().await {
        Some(domain) => Json(serde_json::json!({"domain": domain})),
        None => Json(serde_json::json!({"error": "No domains tracked"})),
    }
}

/// GET /api/source-latency/rank - Rank all domains by latency
async fn rank_domains_by_latency_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let ranked = state.manager.rank_domains_by_latency().await;
    Json(serde_json::to_value(ranked).unwrap_or_default())
}

/// POST /api/source-latency/clear/:domain - Clear latency data for a specific domain
async fn clear_source_latency_domain_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    state.manager.clear_source_latency_domain(&domain).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/source-latency/clear - Clear all source latency data
async fn clear_source_latency_all_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_source_latency_all().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/source-latency/decay - Apply periodic decay to all domains
async fn apply_source_latency_decay_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.apply_source_latency_decay().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ── Phase 151: Bandwidth QoS REST API Handlers ─────────────────────────────

/// GET /api/bandwidth-qos - Get bandwidth QoS configuration
async fn get_bandwidth_qos_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_bandwidth_qos_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/bandwidth-qos - Update bandwidth QoS configuration
async fn set_bandwidth_qos_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::bandwidth_qos::BandwidthQosConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_bandwidth_qos_config(config).await;
    let _ = state.manager.save_bandwidth_qos_config().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/bandwidth-qos/summary - Get bandwidth QoS summary
async fn get_bandwidth_qos_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_bandwidth_qos_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// POST /api/bandwidth-qos/assign/:task_id - Assign QoS tier to a task
async fn assign_qos_tier_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let tier_str = body
        .get("tier")
        .and_then(|v| v.as_str())
        .unwrap_or("normal");
    match crate::bandwidth_qos::QosTier::parse(tier_str) {
        Some(tier) => match state.manager.assign_qos_tier(&task_id, tier).await {
            Ok(()) => {
                let _ = state.manager.save_bandwidth_qos_config().await;
                Json(serde_json::json!({"status": "ok", "task_id": task_id, "tier": tier_str}))
            }
            Err(e) => Json(serde_json::json!({"error": e.to_string()})),
        },
        None => Json(serde_json::json!({"error": format!("Invalid tier: {}", tier_str)})),
    }
}

/// POST /api/bandwidth-qos/assign/:task_id/remove - Remove QoS assignment
async fn remove_qos_assignment_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.remove_qos_assignment(&task_id).await {
        Ok(()) => {
            let _ = state.manager.save_bandwidth_qos_config().await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/bandwidth-qos/task/:task_id - Get QoS tier for a task
async fn get_task_qos_tier_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let tier = state.manager.get_task_qos_tier(&task_id).await;
    let weight = state.manager.get_task_qos_weight(&task_id).await;
    Json(serde_json::json!({
        "task_id": task_id,
        "tier": format!("{:?}", tier).to_lowercase(),
        "weight": weight,
    }))
}

/// GET /api/bandwidth-qos/rules - List all QoS rules
async fn list_qos_rules_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let rules = state.manager.list_qos_rules().await;
    Json(serde_json::to_value(rules).unwrap_or_default())
}

/// POST /api/bandwidth-qos/rules - Add a QoS auto-classification rule
async fn add_qos_rule_handler(
    State(state): State<Arc<WebState>>,
    Json(rule): Json<crate::bandwidth_qos::QosAutoRule>,
) -> Json<serde_json::Value> {
    match state.manager.add_qos_rule(rule).await {
        Ok(()) => {
            let _ = state.manager.save_bandwidth_qos_config().await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// DELETE /api/bandwidth-qos/rules/:rule_id - Remove a QoS rule
async fn remove_qos_rule_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.remove_qos_rule(&rule_id).await {
        Ok(_) => {
            let _ = state.manager.save_bandwidth_qos_config().await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/bandwidth-qos/rules/:rule_id/enable - Enable/disable a QoS rule
async fn set_qos_rule_enabled_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match state.manager.set_qos_rule_enabled(&rule_id, enabled).await {
        Ok(()) => {
            let _ = state.manager.save_bandwidth_qos_config().await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/bandwidth-qos/rules/:rule_id/priority - Set rule priority
async fn set_qos_rule_priority_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(rule_id): axum::extract::Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let priority = body.get("priority").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    match state
        .manager
        .set_qos_rule_priority(&rule_id, priority)
        .await
    {
        Ok(()) => {
            let _ = state.manager.save_bandwidth_qos_config().await;
            Json(serde_json::json!({"status": "ok"}))
        }
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/bandwidth-qos/clear/assignments - Clear all QoS assignments
async fn clear_qos_assignments_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_qos_assignments().await;
    let _ = state.manager.save_bandwidth_qos_config().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/bandwidth-qos/clear/rules - Clear all QoS rules
async fn clear_qos_rules_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_qos_rules().await;
    let _ = state.manager.save_bandwidth_qos_config().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ========== Phase 149: Bandwidth Usage Tracker REST API Handlers ==========

/// GET /api/bandwidth-usage - Get bandwidth usage tracker configuration
async fn get_bandwidth_usage_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_bandwidth_usage_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/bandwidth-usage - Update bandwidth usage tracker configuration
async fn set_bandwidth_usage_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::bandwidth_usage::BandwidthUsageConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_bandwidth_usage_config(config).await;
    let _ = state.manager.save_bandwidth_usage().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/bandwidth-usage/summary - Get bandwidth usage summary
async fn get_bandwidth_usage_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_bandwidth_usage_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/bandwidth-usage/24h - Get rolling 24-hour window summary
async fn get_bandwidth_usage_24h_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_bandwidth_usage_24h().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/bandwidth-usage/peak-hours - Get peak hour analysis
async fn get_bandwidth_usage_peak_hours_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let top_n = params
        .get("top_n")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(5);
    let analysis = state.manager.get_bandwidth_usage_peak_hours(top_n).await;
    Json(serde_json::to_value(analysis).unwrap_or_default())
}

/// POST /api/bandwidth-usage/clear - Clear all bandwidth usage data
async fn clear_bandwidth_usage_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_bandwidth_usage().await;
    let _ = state.manager.save_bandwidth_usage().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/bandwidth-usage/format - Get human-readable bandwidth usage report
async fn get_bandwidth_usage_format_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let report = state.manager.format_bandwidth_usage().await;
    Json(serde_json::json!({"report": report}))
}

// ========== Phase 153: Download Cooldown REST API Handlers ==========

/// GET /api/cooldown - Get cooldown configuration
async fn get_cooldown_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_cooldown_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/cooldown - Update cooldown configuration
async fn set_cooldown_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_cooldown::CooldownConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_cooldown_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/cooldown/status - Get cooldown status for a task
async fn get_cooldown_status_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let task_id = body
        .get("task_id")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    match state.manager.get_cooldown_status(task_id).await {
        Some(status) => Ok(Json(serde_json::to_value(status).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/cooldown/tick - Tick cooldown (move expired tasks back to Queued)
async fn tick_cooldown_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let count = state.manager.tick_cooldown().await;
    Json(serde_json::json!({"tasks_resumed": count}))
}

/// POST /api/cooldown/reset/:task_id - Reset cooldown for a task
async fn reset_cooldown_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    state.manager.reset_task_cooldown(&task_id).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/cooldown/summary - Get cooldown summary for all tasks
async fn get_cooldown_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_cooldown_config().await;
    let tasks = state.manager.list_tasks().await;
    let mut statuses = Vec::new();
    for task in tasks {
        if let Some(ref cooldown_state) = task.cooldown {
            let status =
                crate::download_cooldown::cooldown_status(&task.id, cooldown_state, &config);
            statuses.push(status);
        }
    }
    Json(serde_json::json!({
        "config": config,
        "tasks_in_cooldown": statuses.len(),
        "statuses": statuses
    }))
}

// ========== Phase 153: URL Health Monitor REST API Handlers ==========

/// GET /api/url-health - Get URL health monitor configuration
async fn get_url_health_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_url_health_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/url-health - Update URL health monitor configuration
async fn set_url_health_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::url_health_monitor::UrlHealthMonitorConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_url_health_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/url-health/summary - Get URL health monitoring summary
async fn get_url_health_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let summary = state.manager.get_url_health_summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

/// GET /api/url-health/checks - Get all URL health checks
async fn get_url_health_checks_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let checks = state.manager.get_all_url_health_checks().await;
    Json(serde_json::to_value(checks).unwrap_or_default())
}

/// POST /api/url-health/monitor - Add a URL to monitoring
async fn monitor_url_health_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let url = body
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let success = state.manager.monitor_url_health(url).await;
    if success {
        Ok(Json(serde_json::json!({"status": "ok", "url": url})))
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

/// DELETE /api/url-health/monitor/:url - Remove a URL from monitoring
async fn unmonitor_url_health_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(url): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let success = state.manager.unmonitor_url_health(&url).await;
    Json(serde_json::json!({"removed": success}))
}

/// GET /api/url-health/check/:url - Get health status for a specific URL
async fn get_url_health_check_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(url): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.get_url_health(&url).await {
        Some(check) => Ok(Json(serde_json::to_value(check).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/url-health/cleanup - Remove dead URLs from monitoring
async fn cleanup_dead_urls_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let removed = state.manager.url_health_monitor().cleanup_dead_urls().await;
    Json(serde_json::json!({"removed": removed}))
}

// ========== Phase 153: URL Blacklist REST API Handlers ==========

/// GET /api/url-blacklist - Get URL blacklist configuration
async fn get_url_blacklist_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let config = state.manager.get_url_blacklist_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/url-blacklist - Update URL blacklist configuration
async fn set_url_blacklist_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::url_blacklist::BlacklistConfig>,
) -> Json<serde_json::Value> {
    match state.manager.set_url_blacklist_config(config).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/url-blacklist/enable - Enable or disable URL blacklist
async fn set_url_blacklist_enabled_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    match state.manager.set_blacklist_enabled(enabled).await {
        Ok(()) => Json(serde_json::json!({"status": "ok", "enabled": enabled})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// GET /api/url-blacklist/entries - List all blacklist entries
async fn list_blacklist_entries_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let entries = state.manager.list_blacklist_entries().await;
    Json(serde_json::json!({"entries": entries, "count": entries.len()}))
}

/// POST /api/url-blacklist/entries - Add a new blacklist entry
async fn add_blacklist_entry_handler(
    State(state): State<Arc<WebState>>,
    Json(entry): Json<crate::url_blacklist::BlacklistEntry>,
) -> Json<serde_json::Value> {
    match state.manager.add_blacklist_entry(entry).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// DELETE /api/url-blacklist/entries/:id - Remove a blacklist entry
async fn remove_blacklist_entry_handler(
    State(state): State<Arc<WebState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match state.manager.remove_blacklist_entry(&id).await {
        Ok(()) => Json(serde_json::json!({"status": "ok", "removed": id})),
        Err(e) => Json(serde_json::json!({"error": e.to_string()})),
    }
}

/// POST /api/url-blacklist/check - Check if a URL is blocked
async fn check_url_blocked_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let url = body.get("url").and_then(|v| v.as_str()).unwrap_or("");
    let result = state.manager.check_url_blocked(url).await;
    Json(serde_json::to_value(result).unwrap_or_default())
}

// ========== Phase 154: Dependency Visualization REST API Handlers ==========

/// GET /api/dependency-visualization - Build and return the dependency graph
async fn get_dep_viz_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    match state.manager.build_dependency_visualization().await {
        Some(graph) => Json(serde_json::to_value(graph).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "no graph available"})),
    }
}

/// GET /api/dependency-visualization/stats - Get graph statistics
async fn get_dep_viz_stats_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    match state.manager.get_dep_visualization_stats().await {
        Some(stats) => Json(serde_json::to_value(stats).unwrap_or_default()),
        None => Json(serde_json::json!({"error": "no stats available"})),
    }
}

/// GET /api/dependency-visualization/config - Get visualization config
async fn get_dep_viz_config_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_dep_visualization_config().await;
    Json(serde_json::to_value(config).unwrap_or_default())
}

/// POST /api/dependency-visualization/config - Update visualization config
async fn set_dep_viz_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::dependency_visualization::VisualizationConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_dep_visualization_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/dependency-visualization/cycles - Get detected dependency cycles
async fn get_dep_viz_cycles_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let cycles = state.manager.get_dependency_cycles().await;
    Json(serde_json::json!({"cycles": cycles, "count": cycles.len()}))
}

/// GET /api/dependency-visualization/roots - Get root tasks (no dependencies)
async fn get_dep_viz_roots_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let roots = state.manager.get_dependency_roots().await;
    Json(serde_json::json!({"roots": roots, "count": roots.len()}))
}

/// GET /api/dependency-visualization/leaves - Get leaf tasks (no dependents)
async fn get_dep_viz_leaves_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let leaves = state.manager.get_dependency_leaves().await;
    Json(serde_json::json!({"leaves": leaves, "count": leaves.len()}))
}

/// GET /api/dependency-visualization/text - Get text-based visualization
async fn get_dep_viz_text_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let text = state.manager.visualize_dependency_graph().await;
    Json(serde_json::json!({"text": text}))
}

/// GET /api/dependency-visualization/dot - Export graph in DOT format (Graphviz)
async fn get_dep_viz_dot_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let dot = state.manager.export_dependency_graph_dot().await;
    Json(serde_json::json!({"dot": dot}))
}

// ── Download Diagnostics Handlers (Phase 156) ─────────────────────────

/// GET /api/diagnostics - Get diagnostics summary
async fn get_diagnostics_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let summary = state.manager.get_diagnostics_summary().await;
    Json(serde_json::json!({
        "total_findings": summary.total_findings,
        "findings_by_severity": summary.findings_by_severity,
        "findings_by_category": summary.findings_by_category,
        "critical_count": summary.critical_count,
        "error_count": summary.error_count,
        "warning_count": summary.warning_count,
        "info_count": summary.info_count,
        "health_score": summary.health_score,
        "top_recommendations": summary.top_recommendations
    }))
}

/// GET /api/diagnostics/config - Get diagnostics configuration
async fn get_diagnostics_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let config = state.manager.get_diagnostics_config().await;
    Json(serde_json::json!({
        "enabled": config.enabled,
        "slow_download_threshold_bps": config.slow_download_threshold_bps,
        "stuck_task_threshold_secs": config.stuck_task_threshold_secs,
        "min_disk_space_bytes": config.min_disk_space_bytes,
        "max_retry_threshold": config.max_retry_threshold,
        "max_consecutive_failures": config.max_consecutive_failures,
        "check_network": config.check_network,
        "check_disk": config.check_disk,
        "check_performance": config.check_performance,
        "check_queue": config.check_queue,
        "max_findings_per_category": config.max_findings_per_category
    }))
}

/// POST /api/diagnostics/config - Update diagnostics configuration
async fn set_diagnostics_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::download_diagnostics::DiagnosticsConfig>,
) -> impl axum::response::IntoResponse {
    match state.manager.set_diagnostics_config(config).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

/// POST /api/diagnostics/run - Run diagnostics analysis
async fn run_diagnostics_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let findings = state.manager.run_diagnostics().await;
    let findings_json: Vec<serde_json::Value> = findings
        .iter()
        .map(|f| {
            serde_json::json!({
                "category": f.category.to_string(),
                "severity": f.severity.to_string(),
                "title": f.title,
                "description": f.description,
                "recommendations": f.recommendations,
                "related_task_ids": f.related_task_ids
            })
        })
        .collect();
    Json(serde_json::json!({
        "findings": findings_json,
        "count": findings.len()
    }))
}

/// GET /api/diagnostics/report - Get formatted diagnostics report
async fn get_diagnostics_report_handler(
    State(state): State<Arc<WebState>>,
) -> impl axum::response::IntoResponse {
    let report = state.manager.get_diagnostics_report().await;
    Json(serde_json::json!({"report": report}))
}

// ========== Phase 158: Speed Heatmap REST API Handlers ==========

/// GET /api/speed-heatmap - Get speed heatmap configuration
async fn get_speed_heatmap_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_heatmap::SpeedHeatmapConfig> {
    let config = state.manager.get_speed_heatmap_config().await;
    Json(config)
}

/// POST /api/speed-heatmap - Update speed heatmap configuration
async fn set_speed_heatmap_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::speed_heatmap::SpeedHeatmapConfig>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state.manager.set_speed_heatmap_config(config).await;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// GET /api/speed-heatmap/summary - Get speed heatmap summary
async fn get_speed_heatmap_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::speed_heatmap::SpeedHeatmapSummary> {
    let summary = state.manager.get_speed_heatmap_summary().await;
    Json(summary)
}

/// GET /api/speed-heatmap/report - Get formatted speed heatmap report
async fn get_speed_heatmap_report_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let report = state.manager.format_speed_heatmap_report().await;
    Json(serde_json::json!({"report": report}))
}

/// GET /api/speed-heatmap/hourly/:hour - Get hourly average speed
async fn get_speed_heatmap_hourly_handler(
    State(state): State<Arc<WebState>>,
    Path(hour): Path<u8>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if hour > 23 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let speed = state.manager.get_speed_heatmap_hourly_speed(hour).await;
    Ok(Json(serde_json::json!({
        "hour": hour,
        "avg_speed_bps": speed
    })))
}

/// GET /api/speed-heatmap/daily/:day - Get daily average speed
async fn get_speed_heatmap_daily_handler(
    State(state): State<Arc<WebState>>,
    Path(day): Path<u8>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if day > 6 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let speed = state.manager.get_speed_heatmap_daily_speed(day).await;
    Ok(Json(serde_json::json!({
        "day_of_week": day,
        "avg_speed_bps": speed
    })))
}

/// GET /api/speed-heatmap/quality/:day/:hour - Get quality rating for a time slot
async fn get_speed_heatmap_quality_handler(
    State(state): State<Arc<WebState>>,
    Path((day, hour)): Path<(u8, u8)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if day > 6 || hour > 23 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let quality = state.manager.get_speed_heatmap_quality(day, hour).await;
    Ok(Json(serde_json::json!({
        "day_of_week": day,
        "hour": hour,
        "quality": quality
    })))
}

/// DELETE /api/speed-heatmap - Reset all heatmap data
async fn reset_speed_heatmap_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.reset_speed_heatmap().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/speed-heatmap/prune - Prune old heatmap data
async fn prune_speed_heatmap_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.prune_speed_heatmap().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ========== Phase 159: Progress Prediction REST API Handlers ==========

/// GET /api/progress-prediction - Get progress prediction configuration
async fn get_progress_prediction_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::progress_prediction::PredictionConfig> {
    let config = state.manager.get_prediction_config().await;
    Json(config)
}

/// POST /api/progress-prediction - Update progress prediction configuration
async fn set_progress_prediction_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::progress_prediction::PredictionConfig>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state.manager.set_prediction_config(config).await;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// GET /api/progress-prediction/predict/:task_id - Predict completion time for a task
async fn predict_task_completion_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.manager.predict_task_completion(&task_id).await {
        Some(prediction) => Ok(Json(serde_json::to_value(prediction).unwrap_or_default())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// GET /api/progress-prediction/predict-all - Predict all active tasks
async fn predict_all_tasks_handler(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<crate::progress_prediction::PredictionResult>> {
    let predictions = state.manager.predict_all_active_tasks().await;
    Json(predictions)
}

/// GET /api/progress-prediction/accuracy - Get prediction accuracy summary
async fn get_prediction_accuracy_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::progress_prediction::AccuracySummary> {
    let accuracy = state.manager.get_prediction_accuracy().await;
    Json(accuracy)
}

/// DELETE /api/progress-prediction/task/:task_id - Remove task from prediction system
async fn remove_prediction_task_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Json<serde_json::Value> {
    state.manager.remove_prediction_task(&task_id).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/progress-prediction/clear - Clear all prediction data
async fn clear_prediction_data_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_prediction_data().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ========== Link Rot Detection (Phase 161) ==========

/// GET /api/link-rot - Get link rot detection configuration
async fn get_link_rot_config_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::link_rot::LinkRotConfig> {
    let config = state.manager.get_link_rot_config().await;
    Json(config)
}

/// POST /api/link-rot - Update link rot detection configuration
async fn set_link_rot_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::link_rot::LinkRotConfig>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .set_link_rot_config(config)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// GET /api/link-rot/summary - Get link rot detection summary
async fn get_link_rot_summary_handler(
    State(state): State<Arc<WebState>>,
) -> Json<crate::link_rot::LinkRotSummary> {
    let summary = state.manager.get_link_rot_summary().await;
    Json(summary)
}

/// GET /api/link-rot/report - Get formatted link rot report
async fn get_link_rot_report_handler(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let report = state.manager.get_link_rot_report().await;
    Json(serde_json::json!({"report": report}))
}

/// GET /api/link-rot/:task_id - Get link rot check result for a task
async fn get_link_rot_task_handler(
    State(state): State<Arc<WebState>>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::link_rot::LinkCheckResult>, StatusCode> {
    match state.manager.get_link_rot_result(&task_id).await {
        Some(result) => Ok(Json(result)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/link-rot/clear - Clear all link rot data
async fn clear_link_rot_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    state.manager.clear_link_rot().await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/link-rot/save - Persist link rot data to disk
async fn save_link_rot_handler(
    State(state): State<Arc<WebState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .manager
        .save_link_rot()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// GET /api/link-rot/batch - Get next batch of task IDs to check
async fn get_link_rot_batch_handler(State(state): State<Arc<WebState>>) -> Json<Vec<String>> {
    let batch = state.manager.get_link_rot_batch().await;
    Json(batch)
}

// ===== URL Intelligence API =====

/// GET /api/url-intelligence/config - Get URL intelligence configuration
async fn get_url_intelligence_config(
    State(state): State<Arc<WebState>>,
) -> Json<crate::url_intelligence::UrlIntelligenceConfig> {
    let config = state.manager.get_url_intelligence_config().await;
    Json(config)
}

/// POST /api/url-intelligence/config - Update URL intelligence configuration
async fn set_url_intelligence_config(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::url_intelligence::UrlIntelligenceConfig>,
) -> Json<serde_json::Value> {
    state.manager.set_url_intelligence_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// POST /api/url-intelligence/analyze - Analyze a URL for download recommendations
async fn analyze_url_handler(
    State(state): State<Arc<WebState>>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<crate::url_intelligence::UrlAnalysis>, StatusCode> {
    let url = req
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let analysis = state.manager.analyze_url(url).await;
    Ok(Json(analysis))
}

/// GET /api/url-intelligence/cache - Get URL intelligence cache statistics
async fn get_url_intelligence_cache_size(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    let size = state.manager.get_url_intelligence_cache_size().await;
    Json(serde_json::json!({"cache_size": size}))
}

/// POST /api/url-intelligence/cache/clear - Clear URL intelligence cache
async fn clear_url_intelligence_cache(
    State(state): State<Arc<WebState>>,
) -> Json<serde_json::Value> {
    state.manager.clear_url_intelligence_cache().await;
    Json(serde_json::json!({"status": "ok"}))
}

// ── Save Path Manager Handlers (Phase 162) ──────────────────────────────

/// GET /api/save-path - Get save path configuration
async fn get_save_path_config_handler(State(state): State<Arc<WebState>>) -> Json<SavePathConfig> {
    let config = state.manager.get_save_path_config().await;
    Json(config)
}

/// POST /api/save-path - Update save path configuration
async fn set_save_path_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<SavePathConfig>,
) -> Json<serde_json::Value> {
    state.manager.save_path_manager().set_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/save-path/validate - Validate base save path
async fn validate_save_path_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let result = state.manager.validate_save_path_base().await;
    Json(result)
}

/// GET /api/save-path/predict/:filename - Predict save path for a filename
async fn predict_save_path_handler(
    State(state): State<Arc<WebState>>,
    Path(filename): Path<String>,
) -> Json<serde_json::Value> {
    let path = state.manager.predict_save_path(&filename).await;
    Json(serde_json::json!({
        "filename": filename,
        "predicted_path": path.display().to_string(),
        "category": format!("{:?}", SavePathManager::detect_category(&filename)),
    }))
}

/// GET /api/save-path/category-dirs - Get custom category directories
async fn get_category_dirs_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let config = state.manager.get_save_path_config().await;
    let dirs: serde_json::Map<String, serde_json::Value> = config
        .category_dirs
        .into_iter()
        .map(|(k, v)| (format!("{:?}", k), serde_json::Value::String(v)))
        .collect();
    Json(serde_json::Value::Object(dirs))
}

/// POST /api/save-path/category-dirs - Set custom category directory
async fn set_category_dir_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let category_str = body.get("category").and_then(|v| v.as_str()).unwrap_or("");
    let dir_name = body.get("dir_name").and_then(|v| v.as_str()).unwrap_or("");

    if category_str.is_empty() || dir_name.is_empty() {
        return Json(serde_json::json!({"error": "category and dir_name required"}));
    }

    let category = match category_str {
        "video" => FileCategory::Video,
        "music" => FileCategory::Music,
        "document" => FileCategory::Document,
        "image" => FileCategory::Image,
        "archive" => FileCategory::Archive,
        "program" => FileCategory::Program,
        "other" => FileCategory::Other,
        _ => return Json(serde_json::json!({"error": "invalid category"})),
    };

    state
        .manager
        .set_category_dir(category, dir_name.to_string())
        .await;
    Json(serde_json::json!({"status": "ok"}))
}

/// DELETE /api/save-path/category-dirs/:category - Remove custom category directory
async fn remove_category_dir_handler(
    State(state): State<Arc<WebState>>,
    Path(category_str): Path<String>,
) -> Json<serde_json::Value> {
    let category = match category_str.as_str() {
        "video" => FileCategory::Video,
        "music" => FileCategory::Music,
        "document" => FileCategory::Document,
        "image" => FileCategory::Image,
        "archive" => FileCategory::Archive,
        "program" => FileCategory::Program,
        "other" => FileCategory::Other,
        _ => return Json(serde_json::json!({"error": "invalid category"})),
    };

    let mut config = state.manager.get_save_path_config().await;
    config.category_dirs.remove(&category);
    state.manager.save_path_manager().set_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

// ── Phase 167: Dynamic Priority Handlers ──────────────────────────────────

/// GET /api/dynamic-priority - Get dynamic priority configuration
async fn get_dynamic_priority_config_handler(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let config = state.manager.get_dynamic_priority_config().await;
    Json(config)
}

/// POST /api/dynamic-priority - Update dynamic priority configuration
async fn set_dynamic_priority_config_handler(
    State(state): State<Arc<WebState>>,
    Json(config): Json<crate::dynamic_priority::DynamicPriorityConfig>,
) -> impl IntoResponse {
    state.manager.set_dynamic_priority_config(config).await;
    Json(serde_json::json!({"status": "ok"}))
}

/// GET /api/dynamic-priority/summary - Get dynamic priority summary
async fn get_dynamic_priority_summary_handler(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let summary = state.manager.get_dynamic_priority_summary().await;
    Json(summary)
}

/// POST /api/dynamic-priority/enable - Enable or disable dynamic priority
async fn set_dynamic_priority_enabled_handler(
    State(state): State<Arc<WebState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    match body.get("enabled").and_then(|v| v.as_bool()) {
        Some(enabled) => {
            state.manager.set_dynamic_priority_enabled(enabled).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "ok", "enabled": enabled})),
            )
        }
        None => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing 'enabled' field"})),
        ),
    }
}

/// POST /api/dynamic-priority/run - Run dynamic priority adjustment
async fn run_dynamic_priority_handler(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let adjustments = state.manager.run_dynamic_priority_adjustment().await;
    Json(serde_json::json!({
        "status": "ok",
        "adjustments_count": adjustments.len(),
        "adjustments": adjustments
    }))
}

/// POST /api/dynamic-priority/clear - Clear dynamic priority history
async fn clear_dynamic_priority_handler(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    state.manager.clear_dynamic_priority_history().await;
    Json(serde_json::json!({"status": "ok"}))
}

#[cfg(test)]
mod phase161_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> Arc<WebState> {
        let manager = Arc::new(DownloadManager::new(std::path::PathBuf::from(
            "/tmp/test_phase161",
        )));
        Arc::new(WebState::new(manager))
    }

    #[tokio::test]
    async fn test_export_tasks_json_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/task-export")
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
        assert_eq!(json["status"], "ok");
        assert_eq!(json["count"], 0);
    }

    #[tokio::test]
    async fn test_export_tasks_csv_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/task-export/csv")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_export_history_empty() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/task-export/history")
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
        assert_eq!(json["count"], 0);
    }

    #[tokio::test]
    async fn test_import_tasks_json_invalid() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task-import")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"data": "invalid json", "conflict_strategy": "skip"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_import_tasks_csv_invalid() {
        let state = test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task-import/csv")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"data": "", "conflict_strategy": "skip"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Empty CSV should succeed with 0 tasks
        assert_eq!(response.status(), StatusCode::OK);
    }
}
