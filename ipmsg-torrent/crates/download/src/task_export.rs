//! Download Task Export/Import System
//!
//! Supports exporting and importing download tasks in JSON and CSV formats.
//! Useful for backup, migration, and batch management.
//!
//! Features:
//! - JSON export/import with full task metadata
//! - CSV export with key fields
//! - Export filtering by state, tags, group, time range
//! - Import deduplication (URL matching)
//! - Import conflict handling: skip/overwrite/rename
//! - Export history tracking
//! - Persistent storage to JSON

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

/// Export format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    /// Full metadata in JSON format.
    Json,
    /// Key fields in CSV format.
    Csv,
}

impl Default for ExportFormat {
    fn default() -> Self {
        Self::Json
    }
}

/// Conflict handling strategy on import.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportConflictStrategy {
    /// Skip duplicate tasks.
    Skip,
    /// Overwrite existing tasks.
    Overwrite,
    /// Rename imported tasks.
    Rename,
}

impl Default for ImportConflictStrategy {
    fn default() -> Self {
        Self::Skip
    }
}

/// Filter for selecting which tasks to export.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExportFilter {
    /// Export only tasks in these states (empty = all states).
    pub states: Vec<String>,
    /// Export only tasks with these tags (empty = all tags).
    pub tags: Vec<String>,
    /// Export only tasks in this group (None = all groups).
    pub group: Option<String>,
    /// Export only tasks created after this time.
    pub created_after: Option<DateTime<Utc>>,
    /// Export only tasks created before this time.
    pub created_before: Option<DateTime<Utc>>,
}

impl ExportFilter {
    /// Check if a task matches this filter.
    pub fn matches(
        &self,
        state: &str,
        tags: &[String],
        group: Option<&str>,
        created_at: DateTime<Utc>,
    ) -> bool {
        if !self.states.is_empty() && !self.states.iter().any(|s| s == state) {
            return false;
        }
        if !self.tags.is_empty() && !self.tags.iter().any(|t| tags.contains(t)) {
            return false;
        }
        if let Some(ref g) = self.group {
            if group != Some(g.as_str()) {
                return false;
            }
        }
        if let Some(after) = self.created_after {
            if created_at < after {
                return false;
            }
        }
        if let Some(before) = self.created_before {
            if created_at > before {
                return false;
            }
        }
        true
    }
}

/// A task record for JSON export/import.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportedTask {
    pub id: String,
    pub name: String,
    pub url: String,
    pub protocol: String,
    pub size: u64,
    pub downloaded: u64,
    pub state: String,
    pub tags: Vec<String>,
    pub group: Option<String>,
    pub priority: String,
    pub notes: Option<String>,
    pub speed_limit_bps: Option<u64>,
    pub bandwidth_weight: u8,
    pub expected_checksum: Option<String>,
    pub checksum_algorithm: Option<String>,
    pub save_path: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub mirror_urls: Vec<String>,
    pub max_download_time_secs: Option<u64>,
    pub deadline: Option<DateTime<Utc>>,
    pub sequential_mode: bool,
}

impl From<crate::DownloadTask> for ExportedTask {
    fn from(task: crate::DownloadTask) -> Self {
        Self {
            id: task.id,
            name: task.name,
            url: task.source_url.unwrap_or_default(),
            protocol: format!("{:?}", task.protocol),
            size: task.size,
            downloaded: task.downloaded,
            state: format!("{:?}", task.state),
            tags: task.tags,
            group: task.group,
            priority: format!("{:?}", task.priority),
            notes: task.notes,
            speed_limit_bps: task.speed_limit_bps,
            bandwidth_weight: task.bandwidth_weight,
            expected_checksum: task.expected_checksum,
            checksum_algorithm: task.checksum_algorithm.map(|a| format!("{:?}", a)),
            save_path: task.save_path.to_string_lossy().to_string(),
            created_at: task.created_at,
            updated_at: task.updated_at,
            mirror_urls: task.mirror_urls,
            max_download_time_secs: task.max_download_time_secs,
            deadline: task.deadline,
            sequential_mode: task.sequential_mode,
        }
    }
}

/// A task record for CSV export (key fields only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvTaskRecord {
    pub id: String,
    pub name: String,
    pub url: String,
    pub protocol: String,
    pub state: String,
    pub progress_pct: f64,
    pub size_bytes: u64,
    pub downloaded_bytes: u64,
    pub tags: String,
    pub group: String,
    pub priority: String,
    pub created_at: String,
}

/// Export configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    /// Default export format.
    pub default_format: ExportFormat,
    /// Default import conflict strategy.
    pub default_conflict_strategy: ImportConflictStrategy,
    /// Export directory path.
    pub export_dir: Option<String>,
    /// Maximum export history entries.
    pub max_history_entries: usize,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            default_format: ExportFormat::Json,
            default_conflict_strategy: ImportConflictStrategy::Skip,
            export_dir: None,
            max_history_entries: 50,
        }
    }
}

/// Result of an import operation.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImportResult {
    /// Number of tasks successfully imported.
    pub imported_count: usize,
    /// Number of tasks skipped (duplicates).
    pub skipped_count: usize,
    /// Number of tasks overwritten.
    pub overwritten_count: usize,
    /// Number of tasks renamed.
    pub renamed_count: usize,
    /// Errors encountered during import.
    pub errors: Vec<String>,
}

impl ImportResult {
    /// Format a human-readable summary.
    pub fn format_summary(&self) -> String {
        let mut lines = Vec::new();
        lines.push("📥 Import Summary".to_string());
        lines.push(format!("  ✅ Imported: {}", self.imported_count));
        lines.push(format!("  ⏭️ Skipped: {}", self.skipped_count));
        lines.push(format!("  🔄 Overwritten: {}", self.overwritten_count));
        lines.push(format!("  ✏️ Renamed: {}", self.renamed_count));
        if !self.errors.is_empty() {
            lines.push(format!("  ❌ Errors: {}", self.errors.len()));
            for err in self.errors.iter().take(5) {
                lines.push(format!("    - {}", err));
            }
        }
        lines.join("\n")
    }
}

/// Export history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportHistoryEntry {
    /// When the export was performed.
    pub exported_at: DateTime<Utc>,
    /// Export format used.
    pub format: ExportFormat,
    /// Number of tasks exported.
    pub task_count: usize,
    /// Output file path.
    pub file_path: String,
    /// Filter applied (if any).
    pub filter_description: String,
}

/// Export history tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExportHistory {
    /// History entries (newest first).
    pub entries: Vec<ExportHistoryEntry>,
}

impl ExportHistory {
    /// Add a new entry, trimming if over max.
    pub fn add(&mut self, entry: ExportHistoryEntry, max_entries: usize) {
        self.entries.insert(0, entry);
        if self.entries.len() > max_entries {
            self.entries.truncate(max_entries);
        }
    }

    /// Get the most recent entries.
    pub fn recent(&self, limit: usize) -> &[ExportHistoryEntry] {
        &self.entries[..self.entries.len().min(limit)]
    }
}

/// Generate CSV header line.
pub fn csv_header() -> &'static str {
    "id,name,url,protocol,state,progress_pct,size_bytes,downloaded_bytes,tags,group,priority,created_at"
}

/// Convert a task to a CSV record line.
pub fn task_to_csv_line(task: &ExportedTask) -> String {
    let progress = if task.size > 0 {
        (task.downloaded as f64 / task.size as f64) * 100.0
    } else {
        0.0
    };
    let tags = task.tags.join(";");
    let group = task.group.as_deref().unwrap_or("");
    format!(
        "{},{},{},{},{},{:.1},{},{},{},{},{},{}",
        escape_csv(&task.id),
        escape_csv(&task.name),
        escape_csv(&task.url),
        escape_csv(&task.protocol),
        escape_csv(&task.state),
        progress,
        task.size,
        task.downloaded,
        escape_csv(&tags),
        escape_csv(group),
        escape_csv(&task.priority),
        escape_csv(&task.created_at.to_rfc3339()),
    )
}

/// Escape a CSV field (wrap in quotes if it contains comma, quote, or newline).
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Detect format from file extension.
pub fn detect_format(path: &str) -> Option<ExportFormat> {
    if path.ends_with(".json") {
        Some(ExportFormat::Json)
    } else if path.ends_with(".csv") {
        Some(ExportFormat::Csv)
    } else {
        None
    }
}

/// Parse JSON export data into task records.
pub fn parse_json_export(
    data: &str,
) -> Result<Vec<ExportedTask>, Box<dyn std::error::Error + Send + Sync>> {
    let tasks: Vec<ExportedTask> = serde_json::from_str(data)?;
    Ok(tasks)
}

/// Parse CSV export data into task records (basic parsing).
pub fn parse_csv_export(
    data: &str,
) -> Result<Vec<ExportedTask>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tasks = Vec::new();
    let mut lines = data.lines();

    // Skip header
    let _header = lines.next();

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let fields = parse_csv_line(line);
        if fields.len() >= 12 {
            let size: u64 = fields[6].parse().unwrap_or(0);
            let downloaded: u64 = fields[7].parse().unwrap_or(0);
            let progress: f64 = fields[5].parse().unwrap_or(0.0);
            let actual_downloaded = if progress > 0.0 && size > 0 {
                ((progress / 100.0) * size as f64) as u64
            } else {
                downloaded
            };

            let tags: Vec<String> = if fields[8].is_empty() {
                Vec::new()
            } else {
                fields[8].split(';').map(|s| s.to_string()).collect()
            };

            let group = if fields[9].is_empty() {
                None
            } else {
                Some(fields[9].clone())
            };

            let created_at = chrono::DateTime::parse_from_rfc3339(&fields[11])
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            tasks.push(ExportedTask {
                id: fields[0].clone(),
                name: fields[1].clone(),
                url: fields[2].clone(),
                protocol: fields[3].clone(),
                size,
                downloaded: actual_downloaded,
                state: fields[4].clone(),
                tags,
                group,
                priority: if fields[10].is_empty() {
                    "normal".to_string()
                } else {
                    fields[10].clone()
                },
                notes: None,
                speed_limit_bps: None,
                bandwidth_weight: 1,
                expected_checksum: None,
                checksum_algorithm: None,
                save_path: String::new(),
                created_at,
                updated_at: created_at,
                mirror_urls: Vec::new(),
                max_download_time_secs: None,
                deadline: None,
                sequential_mode: false,
            });
        }
    }
    Ok(tasks)
}

/// Parse a single CSV line, handling quoted fields.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == ',' {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields
}

/// Compute a set of existing URLs for deduplication.
pub fn build_url_set(urls: &[String]) -> HashSet<String> {
    urls.iter()
        .map(|u| u.to_lowercase().trim_end_matches('/').to_string())
        .collect()
}

/// Check if a URL already exists in the set (case-insensitive, trailing slash tolerant).
pub fn is_duplicate_url(url_set: &HashSet<String>, url: &str) -> bool {
    let normalized = url.to_lowercase();
    let normalized = normalized.trim_end_matches('/');
    url_set.contains(normalized)
}

/// Generate a unique filename for export.
pub fn generate_export_filename(format: ExportFormat) -> String {
    let now = Utc::now();
    let ext = match format {
        ExportFormat::Json => "json",
        ExportFormat::Csv => "csv",
    };
    format!("tasks_export_{}.{}", now.format("%Y%m%d_%H%M%S"), ext)
}

/// Save export config to disk.
pub async fn save_export_config(
    config: &ExportConfig,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(config)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load export config from disk.
pub async fn load_export_config(
    path: &Path,
) -> Result<ExportConfig, Box<dyn std::error::Error + Send + Sync>> {
    let json = tokio::fs::read_to_string(path).await?;
    let config: ExportConfig = serde_json::from_str(&json)?;
    Ok(config)
}

/// Save export history to disk.
pub async fn save_export_history(
    history: &ExportHistory,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(history)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load export history from disk.
pub async fn load_export_history(
    path: &Path,
) -> Result<ExportHistory, Box<dyn std::error::Error + Send + Sync>> {
    let json = tokio::fs::read_to_string(path).await?;
    let history: ExportHistory = serde_json::from_str(&json)?;
    Ok(history)
}

/// Export tasks to a JSON file.
///
/// Writes all provided tasks to the given path in JSON format.
/// Returns the number of tasks exported.
pub fn export_tasks(
    tasks: &[crate::DownloadTask],
    output_path: &Path,
    _description: Option<String>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let exported: Vec<ExportedTask> = tasks.iter().cloned().map(ExportedTask::from).collect();
    let json = serde_json::to_string_pretty(&exported)?;
    std::fs::write(output_path, json)?;
    Ok(exported.len())
}

/// Import tasks from a JSON or CSV file.
///
/// Auto-detects format based on file extension.
/// Returns a list of exported task records.
pub fn import_tasks(
    input_path: &Path,
) -> Result<Vec<ExportedTask>, Box<dyn std::error::Error + Send + Sync>> {
    let data = std::fs::read_to_string(input_path)?;
    let ext = input_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("json");
    match ext {
        "csv" => parse_csv_export(&data),
        _ => parse_json_export(&data),
    }
}

/// Prepare imported tasks for re-import.
///
/// For each exported task, generates a new ID and extracts the source URL
/// (if available) for re-adding via DownloadManager.
/// Returns tuples of (ExportedTask, new_id, source_url).
pub fn prepare_imported_tasks(
    exported: Vec<ExportedTask>,
) -> Vec<(ExportedTask, String, Option<String>)> {
    exported
        .into_iter()
        .map(|task| {
            let new_id = uuid::Uuid::new_v4().to_string();
            let source_url = if task.url.is_empty() {
                None
            } else {
                Some(task.url.clone())
            };
            (task, new_id, source_url)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_task() -> ExportedTask {
        ExportedTask {
            id: "task-001".to_string(),
            name: "Test File".to_string(),
            url: "https://example.com/file.zip".to_string(),
            protocol: "http".to_string(),
            size: 1024000,
            downloaded: 512000,
            state: "downloading".to_string(),
            tags: vec!["test".to_string(), "sample".to_string()],
            group: Some("downloads".to_string()),
            priority: "normal".to_string(),
            notes: Some("A test task".to_string()),
            speed_limit_bps: Some(1048576),
            bandwidth_weight: 2,
            expected_checksum: Some("abc123".to_string()),
            checksum_algorithm: Some("sha256".to_string()),
            save_path: "/tmp/downloads".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            mirror_urls: vec!["https://mirror.example.com/file.zip".to_string()],
            max_download_time_secs: Some(3600),
            deadline: None,
            sequential_mode: false,
        }
    }

    #[test]
    fn test_export_format_default() {
        assert_eq!(ExportFormat::default(), ExportFormat::Json);
    }

    #[test]
    fn test_import_conflict_strategy_default() {
        assert_eq!(
            ImportConflictStrategy::default(),
            ImportConflictStrategy::Skip
        );
    }

    #[test]
    fn test_export_filter_matches_all() {
        let filter = ExportFilter::default();
        assert!(filter.matches("downloading", &[], None, Utc::now()));
    }

    #[test]
    fn test_export_filter_by_state() {
        let filter = ExportFilter {
            states: vec!["complete".to_string()],
            ..Default::default()
        };
        assert!(!filter.matches("downloading", &[], None, Utc::now()));
        assert!(filter.matches("complete", &[], None, Utc::now()));
    }

    #[test]
    fn test_export_filter_by_tags() {
        let filter = ExportFilter {
            tags: vec!["movies".to_string()],
            ..Default::default()
        };
        assert!(!filter.matches("downloading", &["music".to_string()], None, Utc::now()));
        assert!(filter.matches("downloading", &["movies".to_string()], None, Utc::now()));
    }

    #[test]
    fn test_export_filter_by_group() {
        let filter = ExportFilter {
            group: Some("work".to_string()),
            ..Default::default()
        };
        assert!(!filter.matches("downloading", &[], Some("personal"), Utc::now()));
        assert!(filter.matches("downloading", &[], Some("work"), Utc::now()));
    }

    #[test]
    fn test_export_filter_by_time() {
        let now = Utc::now();
        let filter = ExportFilter {
            created_after: Some(now - chrono::Duration::hours(1)),
            ..Default::default()
        };
        assert!(!filter.matches("downloading", &[], None, now - chrono::Duration::hours(2)));
        assert!(filter.matches("downloading", &[], None, now));
    }

    #[test]
    fn test_csv_header() {
        let header = csv_header();
        assert!(header.contains("id"));
        assert!(header.contains("url"));
        assert!(header.contains("progress_pct"));
    }

    #[test]
    fn test_task_to_csv_line() {
        let task = sample_task();
        let line = task_to_csv_line(&task);
        assert!(line.contains("task-001"));
        assert!(line.contains("Test File"));
        assert!(line.contains("https://example.com/file.zip"));
    }

    #[test]
    fn test_escape_csv_no_special() {
        assert_eq!(escape_csv("hello"), "hello");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv("hello,world"), "\"hello,world\"");
    }

    #[test]
    fn test_escape_csv_with_quotes() {
        assert_eq!(escape_csv("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn test_detect_format() {
        assert_eq!(detect_format("export.json"), Some(ExportFormat::Json));
        assert_eq!(detect_format("export.csv"), Some(ExportFormat::Csv));
        assert_eq!(detect_format("export.txt"), None);
    }

    #[test]
    fn test_parse_json_export() {
        let tasks = vec![sample_task()];
        let json = serde_json::to_string(&tasks).unwrap();
        let parsed = parse_json_export(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "task-001");
    }

    #[test]
    fn test_parse_csv_line_simple() {
        let fields = parse_csv_line("a,b,c");
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_line_quoted() {
        let fields = parse_csv_line("\"hello,world\",b,c");
        assert_eq!(fields, vec!["hello,world", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_line_escaped_quotes() {
        let fields = parse_csv_line("\"say \"\"hi\"\"\",b");
        assert_eq!(fields, vec!["say \"hi\"", "b"]);
    }

    #[test]
    fn test_parse_csv_export() {
        let task = sample_task();
        let header = csv_header();
        let line = task_to_csv_line(&task);
        let csv = format!("{}\n{}", header, line);
        let parsed = parse_csv_export(&csv).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "task-001");
        assert_eq!(parsed[0].name, "Test File");
    }

    #[test]
    fn test_build_url_set() {
        let urls = vec![
            "https://example.com/file.zip".to_string(),
            "https://other.com/data.tar".to_string(),
        ];
        let set = build_url_set(&urls);
        assert!(is_duplicate_url(&set, "https://example.com/file.zip"));
        assert!(is_duplicate_url(&set, "https://example.com/file.zip/"));
        assert!(is_duplicate_url(&set, "HTTPS://EXAMPLE.COM/FILE.ZIP"));
        assert!(!is_duplicate_url(&set, "https://other.com/other.zip"));
    }

    #[test]
    fn test_generate_export_filename() {
        let name = generate_export_filename(ExportFormat::Json);
        assert!(name.starts_with("tasks_export_"));
        assert!(name.ends_with(".json"));

        let name_csv = generate_export_filename(ExportFormat::Csv);
        assert!(name_csv.ends_with(".csv"));
    }

    #[test]
    fn test_import_result_summary() {
        let result = ImportResult {
            imported_count: 5,
            skipped_count: 2,
            overwritten_count: 1,
            renamed_count: 0,
            errors: vec!["parse error".to_string()],
        };
        let summary = result.format_summary();
        assert!(summary.contains("Imported: 5"));
        assert!(summary.contains("Skipped: 2"));
        assert!(summary.contains("Errors: 1"));
    }

    #[test]
    fn test_export_history() {
        let mut history = ExportHistory::default();
        assert!(history.entries.is_empty());

        history.add(
            ExportHistoryEntry {
                exported_at: Utc::now(),
                format: ExportFormat::Json,
                task_count: 10,
                file_path: "/tmp/export.json".to_string(),
                filter_description: "all tasks".to_string(),
            },
            50,
        );
        assert_eq!(history.entries.len(), 1);
    }

    #[test]
    fn test_export_history_max_entries() {
        let mut history = ExportHistory::default();
        for i in 0..10 {
            history.add(
                ExportHistoryEntry {
                    exported_at: Utc::now(),
                    format: ExportFormat::Json,
                    task_count: i,
                    file_path: format!("/tmp/export_{}.json", i),
                    filter_description: "all".to_string(),
                },
                5,
            );
        }
        assert_eq!(history.entries.len(), 5);
    }

    #[test]
    fn test_export_history_recent() {
        let mut history = ExportHistory::default();
        for i in 0..10 {
            history.add(
                ExportHistoryEntry {
                    exported_at: Utc::now(),
                    format: ExportFormat::Json,
                    task_count: i,
                    file_path: format!("/tmp/export_{}.json", i),
                    filter_description: "all".to_string(),
                },
                50,
            );
        }
        let recent = history.recent(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].task_count, 9); // newest first
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = ExportConfig {
            default_format: ExportFormat::Csv,
            default_conflict_strategy: ImportConflictStrategy::Overwrite,
            export_dir: Some("/tmp/exports".to_string()),
            max_history_entries: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ExportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.default_format, ExportFormat::Csv);
        assert_eq!(
            loaded.default_conflict_strategy,
            ImportConflictStrategy::Overwrite
        );
        assert_eq!(loaded.export_dir, Some("/tmp/exports".to_string()));
    }

    #[test]
    fn test_exported_task_serialization_roundtrip() {
        let task = sample_task();
        let json = serde_json::to_string(&task).unwrap();
        let loaded: ExportedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.id, task.id);
        assert_eq!(loaded.name, task.name);
        assert_eq!(loaded.url, task.url);
        assert_eq!(loaded.tags, task.tags);
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("export_config.json");

        let config = ExportConfig {
            default_format: ExportFormat::Csv,
            ..Default::default()
        };
        save_export_config(&config, &path).await.unwrap();

        let loaded = load_export_config(&path).await.unwrap();
        assert_eq!(loaded.default_format, ExportFormat::Csv);
    }

    #[tokio::test]
    async fn test_save_load_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("export_history.json");

        let mut history = ExportHistory::default();
        history.add(
            ExportHistoryEntry {
                exported_at: Utc::now(),
                format: ExportFormat::Json,
                task_count: 5,
                file_path: "/tmp/test.json".to_string(),
                filter_description: "all".to_string(),
            },
            50,
        );
        save_export_history(&history, &path).await.unwrap();

        let loaded = load_export_history(&path).await.unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].task_count, 5);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let result = load_export_config(&path).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_csv_export_empty() {
        let csv = csv_header().to_string();
        let parsed = parse_csv_export(&csv).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_parse_csv_export_multiple_lines() {
        let task1 = sample_task();
        let mut task2 = sample_task();
        task2.id = "task-002".to_string();
        task2.name = "Second File".to_string();

        let header = csv_header();
        let line1 = task_to_csv_line(&task1);
        let line2 = task_to_csv_line(&task2);
        let csv = format!("{}\n{}\n{}", header, line1, line2);
        let parsed = parse_csv_export(&csv).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].id, "task-001");
        assert_eq!(parsed[1].id, "task-002");
    }
}
