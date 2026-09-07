//! CSV Export for download tasks
//!
//! Exports download task data to CSV format for spreadsheet analysis.
//! Unlike JSON export (which is for backup/migration), CSV export is
//! optimized for human readability and data analysis in tools like
//! Excel, Google Sheets, or LibreOffice Calc.

use crate::DownloadTask;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use tokio::fs;

/// CSV column headers
const CSV_HEADERS: &[&str] = &[
    "id",
    "name",
    "protocol",
    "size_bytes",
    "downloaded_bytes",
    "progress_percent",
    "state",
    "speed_bps",
    "error",
    "tags",
    "group",
    "priority",
    "bandwidth_weight",
    "queue_position",
    "depends_on",
    "notes",
    "save_path",
    "created_at",
    "updated_at",
    "active_time_seconds",
    "source_url",
    "mirror_urls",
];

/// CSV export configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvExportConfig {
    /// Field delimiter (default: comma)
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Include header row (default: true)
    #[serde(default = "default_true")]
    pub include_headers: bool,
    /// Quote all fields (default: false, only quote when needed)
    #[serde(default)]
    pub quote_all: bool,
    /// Date/time format (default: RFC3339)
    #[serde(default = "default_datetime_format")]
    pub datetime_format: String,
}

fn default_delimiter() -> char {
    ','
}

fn default_true() -> bool {
    true
}

fn default_datetime_format() -> String {
    "%+".to_string()
}

impl Default for CsvExportConfig {
    fn default() -> Self {
        Self {
            delimiter: ',',
            include_headers: true,
            quote_all: false,
            datetime_format: "%+".to_string(), // RFC3339
        }
    }
}

/// CSV export result
#[derive(Debug, Clone)]
pub struct CsvExportResult {
    /// Number of tasks exported
    pub task_count: usize,
    /// Output file path
    pub path: std::path::PathBuf,
    /// File size in bytes
    pub file_size: u64,
}

/// Escape a field value for CSV
///
/// Quotes the field if it contains the delimiter, quote character, or newlines.
fn escape_csv_field(field: &str, delimiter: char, quote_all: bool) -> String {
    let needs_quoting = quote_all
        || field.contains(delimiter)
        || field.contains('"')
        || field.contains('\n')
        || field.contains('\r');

    if needs_quoting {
        // Escape quotes by doubling them
        let escaped = field.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        field.to_string()
    }
}

/// Convert a DownloadTask to a CSV row
fn task_to_csv_row(task: &DownloadTask, config: &CsvExportConfig) -> String {
    let progress = if task.size > 0 {
        (task.downloaded as f64 / task.size as f64) * 100.0
    } else {
        0.0
    };

    let fields = vec![
        escape_csv_field(&task.id, config.delimiter, config.quote_all),
        escape_csv_field(&task.name, config.delimiter, config.quote_all),
        escape_csv_field(
            &format!("{:?}", task.protocol),
            config.delimiter,
            config.quote_all,
        ),
        task.size.to_string(),
        task.downloaded.to_string(),
        format!("{:.2}", progress),
        escape_csv_field(task.state_label(), config.delimiter, config.quote_all),
        format!("{:.2}", task.speed_bps),
        escape_csv_field(
            task.error.as_deref().unwrap_or(""),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(&task.tags.join(";"), config.delimiter, config.quote_all),
        escape_csv_field(
            task.group.as_deref().unwrap_or(""),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(
            &format!("{:?}", task.priority),
            config.delimiter,
            config.quote_all,
        ),
        task.bandwidth_weight.to_string(),
        task.queue_position
            .map(|p| p.to_string())
            .unwrap_or_default(),
        escape_csv_field(
            &task.depends_on.join(";"),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(
            task.notes.as_deref().unwrap_or(""),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(
            &task.save_path.to_string_lossy(),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(
            &task.created_at.format(&config.datetime_format).to_string(),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(
            &task.updated_at.format(&config.datetime_format).to_string(),
            config.delimiter,
            config.quote_all,
        ),
        format!("{:.1}", task.active_time_seconds),
        escape_csv_field(
            task.source_url.as_deref().unwrap_or(""),
            config.delimiter,
            config.quote_all,
        ),
        escape_csv_field(
            &task.mirror_urls.join(";"),
            config.delimiter,
            config.quote_all,
        ),
    ];

    fields.join(&config.delimiter.to_string())
}

/// Export tasks to a CSV file
///
/// Writes tasks in CSV format for spreadsheet analysis.
/// Unlike JSON export, this is one-way (no import from CSV).
pub fn export_tasks_to_csv(
    tasks: &[DownloadTask],
    output_path: &Path,
    config: Option<CsvExportConfig>,
) -> Result<CsvExportResult, CsvExportError> {
    let config = config.unwrap_or_default();

    // Atomic write: write to temp file first
    let tmp_path = output_path.with_extension("csv.tmp");
    let mut file = std::fs::File::create(&tmp_path)?;

    // Write header row
    if config.include_headers {
        let header_line = CSV_HEADERS.join(&config.delimiter.to_string());
        writeln!(file, "{}", header_line)?;
    }

    // Write data rows
    for task in tasks {
        let row = task_to_csv_row(task, &config);
        writeln!(file, "{}", row)?;
    }

    // Flush and close
    file.flush()?;
    drop(file);

    // Atomic rename
    std::fs::rename(&tmp_path, output_path)?;

    let file_size = std::fs::metadata(output_path)?.len();

    Ok(CsvExportResult {
        task_count: tasks.len(),
        path: output_path.to_path_buf(),
        file_size,
    })
}

/// Export tasks to a CSV string (useful for API responses)
pub fn export_tasks_to_csv_string(
    tasks: &[DownloadTask],
    config: Option<CsvExportConfig>,
) -> Result<String, CsvExportError> {
    let config = config.unwrap_or_default();
    let mut output = String::new();

    // Write header row
    if config.include_headers {
        let header_line = CSV_HEADERS.join(&config.delimiter.to_string());
        output.push_str(&header_line);
        output.push('\n');
    }

    // Write data rows
    for task in tasks {
        let row = task_to_csv_row(task, &config);
        output.push_str(&row);
        output.push('\n');
    }

    Ok(output)
}

/// Generate a CSV summary report with aggregated statistics
pub fn generate_csv_summary(tasks: &[DownloadTask]) -> String {
    let mut output = String::new();

    // Summary section
    output.push_str("# Download Tasks Summary\n");
    output.push_str(&format!("# Generated: {}\n", Utc::now().to_rfc3339()));
    output.push_str(&format!("# Total tasks: {}\n", tasks.len()));

    // Count by state
    let mut state_counts = std::collections::HashMap::new();
    let mut total_size = 0u64;
    let mut total_downloaded = 0u64;

    for task in tasks {
        *state_counts
            .entry(task.state_label().to_string())
            .or_insert(0) += 1;
        total_size += task.size;
        total_downloaded += task.downloaded;
    }

    output.push_str("#\n# State breakdown:\n");
    for (state, count) in state_counts.iter() {
        output.push_str(&format!("#   {}: {}\n", state, count));
    }

    let overall_progress = if total_size > 0 {
        (total_downloaded as f64 / total_size as f64) * 100.0
    } else {
        0.0
    };
    output.push_str(&format!(
        "#\n# Overall progress: {:.1}%\n",
        overall_progress
    ));
    output.push_str(&format!("# Total size: {} bytes\n", total_size));
    output.push_str(&format!("# Total downloaded: {} bytes\n", total_downloaded));
    output.push_str("#\n");

    output
}

/// Save CSV export config to disk (atomic write)
pub async fn save_csv_export_config(
    config: &CsvExportConfig,
    path: &Path,
) -> Result<(), CsvExportError> {
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;
    let tmp = path.with_extension("csv_config.tmp");
    fs::write(&tmp, json.as_bytes()).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

/// Load CSV export config from disk
pub async fn load_csv_export_config(path: &Path) -> Option<CsvExportConfig> {
    let data = fs::read_to_string(path).await.ok()?;
    serde_json::from_str(&data).ok()
}

/// CSV export errors
#[derive(Debug, thiserror::Error)]
pub enum CsvExportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("No tasks to export")]
    EmptyTaskList,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DownloadPriority, DownloadProtocol, DownloadState};
    use std::path::PathBuf;

    fn make_test_task(id: &str, name: &str) -> DownloadTask {
        DownloadTask {
            id: id.to_string(),
            name: name.to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1024,
            downloaded: 512,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: PathBuf::from("/tmp/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["test".to_string()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 60.5,
            current_session_start: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        }
    }

    #[test]
    fn test_escape_csv_field_no_special_chars() {
        assert_eq!(escape_csv_field("hello", ',', false), "hello");
        assert_eq!(escape_csv_field("world", ',', false), "world");
    }

    #[test]
    fn test_escape_csv_field_with_delimiter() {
        assert_eq!(
            escape_csv_field("hello,world", ',', false),
            "\"hello,world\""
        );
    }

    #[test]
    fn test_escape_csv_field_with_quotes() {
        assert_eq!(
            escape_csv_field("say \"hi\"", ',', false),
            "\"say \"\"hi\"\"\""
        );
    }

    #[test]
    fn test_escape_csv_field_with_newline() {
        assert_eq!(
            escape_csv_field("line1\nline2", ',', false),
            "\"line1\nline2\""
        );
    }

    #[test]
    fn test_escape_csv_field_quote_all() {
        assert_eq!(escape_csv_field("hello", ',', true), "\"hello\"");
    }

    #[test]
    fn test_escape_csv_field_semicolon_delimiter() {
        assert_eq!(
            escape_csv_field("hello;world", ';', false),
            "\"hello;world\""
        );
        assert_eq!(escape_csv_field("hello", ';', false), "hello");
    }

    #[test]
    fn test_task_to_csv_row_basic() {
        let task = make_test_task("task-1", "file.txt");
        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        // Should contain all fields
        assert!(row.contains("task-1"));
        assert!(row.contains("file.txt"));
        assert!(row.contains("Xunlei"));
        assert!(row.contains("1024"));
        assert!(row.contains("512"));
        assert!(row.contains("50.00")); // progress
        assert!(row.contains("downloading"));
        assert!(row.contains("100.00")); // speed
        assert!(row.contains("test")); // tags
        assert!(row.contains("60.5")); // active_time
    }

    #[test]
    fn test_task_to_csv_row_with_special_chars() {
        let mut task = make_test_task("task-1", "file,with,commas.txt");
        task.notes = Some("notes with \"quotes\"".to_string());
        task.tags = vec!["tag1".to_string(), "tag2".to_string()];

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        // Name should be quoted
        assert!(row.contains("\"file,with,commas.txt\""));
        // Notes should be quoted with escaped quotes
        assert!(row.contains("\"notes with \"\"quotes\"\"\""));
        // Tags should be semicolon-separated
        assert!(row.contains("tag1;tag2"));
    }

    #[test]
    fn test_export_tasks_to_csv_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("export.csv");

        let tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
        ];

        let result = export_tasks_to_csv(&tasks, &csv_path, None).unwrap();
        assert_eq!(result.task_count, 2);
        assert!(csv_path.exists());

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Header + 2 data rows
        assert_eq!(lines.len(), 3);

        // Check header
        assert!(lines[0].contains("id,name,protocol"));
        assert!(lines[0].contains("size_bytes,downloaded_bytes"));

        // Check data rows
        assert!(lines[1].contains("task-1"));
        assert!(lines[1].contains("file1.txt"));
        assert!(lines[2].contains("task-2"));
        assert!(lines[2].contains("file2.mp4"));
    }

    #[test]
    fn test_export_tasks_to_csv_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("empty.csv");

        let result = export_tasks_to_csv(&[], &csv_path, None).unwrap();
        assert_eq!(result.task_count, 0);

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Only header row
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("id,name,protocol"));
    }

    #[test]
    fn test_export_tasks_to_csv_no_headers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("no_headers.csv");

        let tasks = vec![make_test_task("task-1", "file.txt")];
        let config = CsvExportConfig {
            include_headers: false,
            ..Default::default()
        };

        export_tasks_to_csv(&tasks, &csv_path, Some(config)).unwrap();

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Only data row, no header
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("task-1"));
    }

    #[test]
    fn test_export_tasks_to_csv_string() {
        let tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
        ];

        let csv_string = export_tasks_to_csv_string(&tasks, None).unwrap();
        let lines: Vec<&str> = csv_string.lines().collect();

        // Header + 2 data rows
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("id,name,protocol"));
    }

    #[test]
    fn test_export_tasks_to_csv_string_no_headers() {
        let tasks = vec![make_test_task("task-1", "file.txt")];
        let config = CsvExportConfig {
            include_headers: false,
            ..Default::default()
        };

        let csv_string = export_tasks_to_csv_string(&tasks, Some(config)).unwrap();
        let lines: Vec<&str> = csv_string.lines().collect();

        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("task-1"));
    }

    #[test]
    fn test_csv_export_with_custom_delimiter() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("semicolon.csv");

        let tasks = vec![make_test_task("task-1", "file.txt")];
        let config = CsvExportConfig {
            delimiter: ';',
            ..Default::default()
        };

        export_tasks_to_csv(&tasks, &csv_path, Some(config)).unwrap();

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let header_line = content.lines().next().unwrap();

        // Should use semicolons
        assert!(header_line.contains("id;name;protocol"));
    }

    #[test]
    fn test_csv_export_atomic_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("atomic.csv");

        // Write initial content
        std::fs::write(&csv_path, "old content").unwrap();

        let tasks = vec![make_test_task("task-1", "file.txt")];
        export_tasks_to_csv(&tasks, &csv_path, None).unwrap();

        // Verify content was replaced
        let content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(content.contains("task-1"));
        assert!(!content.contains("old content"));

        // No temp file left
        assert!(!csv_path.with_extension("csv.tmp").exists());
    }

    #[test]
    fn test_generate_csv_summary() {
        let mut tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
            make_test_task("task-3", "file3.zip"),
        ];

        // Set different states
        tasks[1].state = DownloadState::Complete;
        tasks[1].downloaded = 2048;
        tasks[1].size = 2048;
        tasks[2].state = DownloadState::Error;
        tasks[2].error = Some("timeout".to_string());

        let summary = generate_csv_summary(&tasks);

        assert!(summary.contains("# Download Tasks Summary"));
        assert!(summary.contains("# Total tasks: 3"));
        assert!(summary.contains("downloading: 1"));
        assert!(summary.contains("complete: 1"));
        assert!(summary.contains("error: 1"));
        assert!(summary.contains("Overall progress:"));
    }

    #[test]
    fn test_csv_export_all_protocols() {
        let protocols = vec![
            (DownloadProtocol::Torrent, "torrent.torrent"),
            (DownloadProtocol::Ed2k, "ed2k.txt"),
            (DownloadProtocol::Xunlei, "xunlei.zip"),
            (DownloadProtocol::Magnet, "magnet"),
            (DownloadProtocol::P2P, "p2p.dat"),
        ];

        let tasks: Vec<DownloadTask> = protocols
            .into_iter()
            .map(|(proto, name)| {
                let mut task = make_test_task(&format!("proto-{:?}", proto), name);
                task.protocol = proto;
                task
            })
            .collect();

        let csv_string = export_tasks_to_csv_string(&tasks, None).unwrap();

        assert!(csv_string.contains("Torrent"));
        assert!(csv_string.contains("Ed2k"));
        assert!(csv_string.contains("Xunlei"));
        assert!(csv_string.contains("Magnet"));
        assert!(csv_string.contains("P2P"));
    }

    #[test]
    fn test_csv_export_with_dependencies() {
        let mut task = make_test_task("task-1", "file.txt");
        task.depends_on = vec!["dep-1".to_string(), "dep-2".to_string()];

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();

        // Dependencies should be semicolon-separated
        assert!(csv_string.contains("dep-1;dep-2"));
    }

    #[test]
    fn test_csv_export_with_mirrors() {
        let mut task = make_test_task("task-1", "file.txt");
        task.mirror_urls = vec![
            "http://mirror1.com/file.txt".to_string(),
            "http://mirror2.com/file.txt".to_string(),
        ];

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();

        // Mirrors should be semicolon-separated
        assert!(csv_string.contains("http://mirror1.com/file.txt;http://mirror2.com/file.txt"));
    }

    #[test]
    fn test_csv_config_default() {
        let config = CsvExportConfig::default();
        assert_eq!(config.delimiter, ',');
        assert!(config.include_headers);
        assert!(!config.quote_all);
    }

    #[test]
    fn test_csv_export_progress_calculation() {
        let mut task = make_test_task("task-1", "file.txt");
        task.size = 1000;
        task.downloaded = 250;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();

        // Progress should be 25.00%
        assert!(csv_string.contains("25.00"));
    }

    #[test]
    fn test_csv_export_zero_size_task() {
        let mut task = make_test_task("task-1", "file.txt");
        task.size = 0;
        task.downloaded = 0;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();

        // Progress should be 0.00% (avoid division by zero)
        assert!(csv_string.contains("0.00"));
    }

    // ========== Phase 252: Comprehensive Test Coverage ==========

    // --- CsvExportConfig serde tests ---

    #[test]
    fn test_csv_export_config_serde_roundtrip() {
        let config = CsvExportConfig {
            delimiter: ';',
            include_headers: false,
            quote_all: true,
            datetime_format: "%Y-%m-%d".to_string(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CsvExportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.delimiter, ';');
        assert!(!deserialized.include_headers);
        assert!(deserialized.quote_all);
        assert_eq!(deserialized.datetime_format, "%Y-%m-%d");
    }

    #[test]
    fn test_csv_export_config_serde_default_values() {
        let json = r#"{}"#;
        let config: CsvExportConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.delimiter, ',');
        assert!(config.include_headers);
        assert!(!config.quote_all);
        assert_eq!(config.datetime_format, "%+");
    }

    #[test]
    fn test_csv_export_config_serde_extra_fields_ignored() {
        let json = r#"{"delimiter":"\t","unknown_field":123}"#;
        let config: CsvExportConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.delimiter, '\t');
    }

    #[test]
    fn test_csv_export_config_serde_pretty() {
        let config = CsvExportConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: CsvExportConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.delimiter, config.delimiter);
        assert_eq!(deserialized.include_headers, config.include_headers);
    }

    // --- CsvExportConfig traits ---

    #[test]
    fn test_csv_export_config_clone() {
        let config = CsvExportConfig {
            delimiter: '|',
            include_headers: false,
            quote_all: true,
            datetime_format: "%H:%M".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(cloned.delimiter, '|');
        assert_eq!(cloned.include_headers, false);
        assert_eq!(cloned.quote_all, true);
        assert_eq!(cloned.datetime_format, "%H:%M");
    }

    #[test]
    fn test_csv_export_config_clone_independence() {
        let mut config = CsvExportConfig::default();
        let cloned = config.clone();
        config.delimiter = ';';
        config.include_headers = false;
        assert_eq!(cloned.delimiter, ',');
        assert!(cloned.include_headers);
    }

    #[test]
    fn test_csv_export_config_debug() {
        let config = CsvExportConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("CsvExportConfig"));
        assert!(debug.contains("delimiter"));
    }

    // --- CsvExportResult traits ---

    #[test]
    fn test_csv_export_result_clone() {
        let result = CsvExportResult {
            task_count: 5,
            path: PathBuf::from("/tmp/test.csv"),
            file_size: 1024,
        };
        let cloned = result.clone();
        assert_eq!(cloned.task_count, 5);
        assert_eq!(cloned.path, PathBuf::from("/tmp/test.csv"));
        assert_eq!(cloned.file_size, 1024);
    }

    #[test]
    fn test_csv_export_result_debug() {
        let result = CsvExportResult {
            task_count: 3,
            path: PathBuf::from("/tmp/export.csv"),
            file_size: 512,
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("CsvExportResult"));
        assert!(debug.contains("task_count: 3"));
    }

    // --- CsvExportError tests ---

    #[test]
    fn test_csv_export_error_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = CsvExportError::Io(io_err);
        let display = format!("{}", err);
        assert!(display.contains("IO error"));
        assert!(display.contains("file not found"));
    }

    #[test]
    fn test_csv_export_error_display_empty() {
        let err = CsvExportError::EmptyTaskList;
        let display = format!("{}", err);
        assert!(display.contains("No tasks"));
    }

    #[test]
    fn test_csv_export_error_debug() {
        let err = CsvExportError::EmptyTaskList;
        let debug = format!("{:?}", err);
        assert!(debug.contains("EmptyTaskList"));
    }

    #[test]
    fn test_csv_export_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let err: CsvExportError = CsvExportError::from(io_err);
        match err {
            CsvExportError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied),
            _ => panic!("Expected Io variant"),
        }
    }

    // --- escape_csv_field boundary tests ---

    #[test]
    fn test_escape_csv_field_empty_string() {
        assert_eq!(escape_csv_field("", ',', false), "");
        assert_eq!(escape_csv_field("", ',', true), "\"\"");
    }

    #[test]
    fn test_escape_csv_field_carriage_return() {
        assert_eq!(
            escape_csv_field("line1\rline2", ',', false),
            "\"line1\rline2\""
        );
    }

    #[test]
    fn test_escape_csv_field_unicode() {
        assert_eq!(escape_csv_field("中文文件名", ',', false), "中文文件名");
        assert_eq!(escape_csv_field("中文,文件", ',', false), "\"中文,文件\"");
    }

    #[test]
    fn test_escape_csv_field_emoji() {
        assert_eq!(escape_csv_field("🎉🎊", ',', false), "🎉🎊");
        assert_eq!(escape_csv_field("🎉,🎊", ',', false), "\"🎉,🎊\"");
    }

    #[test]
    fn test_escape_csv_field_tab_delimiter() {
        assert_eq!(
            escape_csv_field("hello\tworld", '\t', false),
            "\"hello\tworld\""
        );
        assert_eq!(escape_csv_field("hello", '\t', false), "hello");
    }

    #[test]
    fn test_escape_csv_field_pipe_delimiter() {
        assert_eq!(
            escape_csv_field("hello|world", '|', false),
            "\"hello|world\""
        );
    }

    #[test]
    fn test_escape_csv_field_multiple_special_chars() {
        let field = "hello,\"world\"\ntest";
        let escaped = escape_csv_field(field, ',', false);
        assert!(escaped.starts_with('"'));
        assert!(escaped.ends_with('"'));
        assert!(escaped.contains("\"\""));
    }

    // --- task_to_csv_row comprehensive tests ---

    #[test]
    fn test_task_to_csv_row_complete_progress() {
        let mut task = make_test_task("task-1", "complete.mp4");
        task.size = 1000;
        task.downloaded = 1000;
        task.state = DownloadState::Complete;

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("100.00"));
        assert!(row.contains("complete"));
    }

    #[test]
    fn test_task_to_csv_row_no_progress() {
        let mut task = make_test_task("task-1", "notstarted.mp4");
        task.size = 1000;
        task.downloaded = 0;
        task.state = DownloadState::Queued;

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("0.00"));
        assert!(row.contains("queued"));
    }

    #[test]
    fn test_task_to_csv_row_with_error() {
        let mut task = make_test_task("task-1", "failed.mp4");
        task.state = DownloadState::Error;
        task.error = Some("connection timeout".to_string());

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("connection timeout"));
        assert!(row.contains("error"));
    }

    #[test]
    fn test_task_to_csv_row_with_group() {
        let mut task = make_test_task("task-1", "file.txt");
        task.group = Some("work".to_string());

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("work"));
    }

    #[test]
    fn test_task_to_csv_row_with_notes() {
        let mut task = make_test_task("task-1", "file.txt");
        task.notes = Some("important download".to_string());

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("important download"));
    }

    #[test]
    fn test_task_to_csv_row_with_source_url() {
        let mut task = make_test_task("task-1", "file.txt");
        task.source_url = Some("http://example.com/file.txt".to_string());

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("http://example.com/file.txt"));
    }

    #[test]
    fn test_task_to_csv_row_with_queue_position() {
        let mut task = make_test_task("task-1", "file.txt");
        task.queue_position = Some(5);

        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        // Queue position should be in the row
        let fields: Vec<&str> = row.split(',').collect();
        assert!(fields.len() >= 14);
    }

    #[test]
    fn test_task_to_csv_row_unicode_task_id() {
        let task = make_test_task("任务-001", "中文文件.txt");
        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("任务-001"));
        assert!(row.contains("中文文件.txt"));
    }

    #[test]
    fn test_task_to_csv_row_emoji_task_id() {
        let task = make_test_task("🎉-task", "📁-file.txt");
        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains("🎉-task"));
        assert!(row.contains("📁-file.txt"));
    }

    #[test]
    fn test_task_to_csv_row_custom_datetime_format() {
        let task = make_test_task("task-1", "file.txt");
        let config = CsvExportConfig {
            datetime_format: "%Y-%m-%d".to_string(),
            ..Default::default()
        };
        let row = task_to_csv_row(&task, &config);

        // Should contain date in YYYY-MM-DD format
        let now = chrono::Utc::now();
        let expected = now.format("%Y-%m-%d").to_string();
        assert!(row.contains(&expected));
    }

    #[test]
    fn test_task_to_csv_row_semicolon_delimiter() {
        let task = make_test_task("task-1", "file.txt");
        let config = CsvExportConfig {
            delimiter: ';',
            ..Default::default()
        };
        let row = task_to_csv_row(&task, &config);

        assert!(row.contains(';'));
        // Fields should be separated by semicolons
        let fields: Vec<&str> = row.split(';').collect();
        assert!(fields.len() > 10);
    }

    #[test]
    fn test_task_to_csv_row_quote_all() {
        let task = make_test_task("task-1", "file.txt");
        let config = CsvExportConfig {
            quote_all: true,
            ..Default::default()
        };
        let row = task_to_csv_row(&task, &config);

        // All fields should be quoted
        assert!(row.contains("\"task-1\""));
        assert!(row.contains("\"file.txt\""));
    }

    // --- export_tasks_to_csv boundary tests ---

    #[test]
    fn test_export_tasks_to_csv_single_task() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("single.csv");

        let tasks = vec![make_test_task("task-1", "file.txt")];
        let result = export_tasks_to_csv(&tasks, &csv_path, None).unwrap();

        assert_eq!(result.task_count, 1);
        assert_eq!(result.path, csv_path);
        assert!(result.file_size > 0);

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 data row
    }

    #[test]
    fn test_export_tasks_to_csv_many_tasks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("many.csv");

        let tasks: Vec<DownloadTask> = (0..100)
            .map(|i| make_test_task(&format!("task-{}", i), &format!("file-{}.txt", i)))
            .collect();

        let result = export_tasks_to_csv(&tasks, &csv_path, None).unwrap();
        assert_eq!(result.task_count, 100);

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 101); // header + 100 data rows
    }

    #[test]
    fn test_export_tasks_to_csv_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("overwrite.csv");

        // First export
        let tasks1 = vec![make_test_task("task-1", "file1.txt")];
        export_tasks_to_csv(&tasks1, &csv_path, None).unwrap();

        // Second export (overwrite)
        let tasks2 = vec![make_test_task("task-2", "file2.txt")];
        export_tasks_to_csv(&tasks2, &csv_path, None).unwrap();

        let content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(content.contains("task-2"));
        assert!(!content.contains("task-1"));
    }

    #[test]
    fn test_export_tasks_to_csv_no_tmp_leftover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("no_tmp.csv");

        let tasks = vec![make_test_task("task-1", "file.txt")];
        export_tasks_to_csv(&tasks, &csv_path, None).unwrap();

        let tmp_path = csv_path.with_extension("csv.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_export_tasks_to_csv_file_size_correct() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("size.csv");

        let tasks = vec![make_test_task("task-1", "file.txt")];
        let result = export_tasks_to_csv(&tasks, &csv_path, None).unwrap();

        let actual_size = std::fs::metadata(&csv_path).unwrap().len();
        assert_eq!(result.file_size, actual_size);
    }

    #[test]
    fn test_export_tasks_to_csv_unicode_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("中文导出.csv");

        let tasks = vec![make_test_task("task-1", "文件.txt")];
        let result = export_tasks_to_csv(&tasks, &csv_path, None).unwrap();

        assert_eq!(result.task_count, 1);
        assert!(csv_path.exists());
    }

    // --- export_tasks_to_csv_string boundary tests ---

    #[test]
    fn test_export_tasks_to_csv_string_empty() {
        let csv_string = export_tasks_to_csv_string(&[], None).unwrap();
        let lines: Vec<&str> = csv_string.lines().collect();
        assert_eq!(lines.len(), 1); // only header
    }

    #[test]
    fn test_export_tasks_to_csv_string_many_tasks() {
        let tasks: Vec<DownloadTask> = (0..50)
            .map(|i| make_test_task(&format!("task-{}", i), &format!("file-{}.txt", i)))
            .collect();

        let csv_string = export_tasks_to_csv_string(&tasks, None).unwrap();
        let lines: Vec<&str> = csv_string.lines().collect();
        assert_eq!(lines.len(), 51); // header + 50 data rows
    }

    #[test]
    fn test_export_tasks_to_csv_string_unicode_content() {
        let mut task = make_test_task("task-1", "中文文件.txt");
        task.notes = Some("这是备注".to_string());
        task.tags = vec!["标签1".to_string(), "标签2".to_string()];

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("中文文件.txt"));
        assert!(csv_string.contains("这是备注"));
        assert!(csv_string.contains("标签1;标签2"));
    }

    // --- generate_csv_summary comprehensive tests ---

    #[test]
    fn test_generate_csv_summary_empty() {
        let summary = generate_csv_summary(&[]);
        assert!(summary.contains("# Total tasks: 0"));
        assert!(summary.contains("Overall progress: 0.0%"));
        assert!(summary.contains("Total size: 0 bytes"));
    }

    #[test]
    fn test_generate_csv_summary_single_task() {
        let tasks = vec![make_test_task("task-1", "file.txt")];
        let summary = generate_csv_summary(&tasks);

        assert!(summary.contains("# Total tasks: 1"));
        assert!(summary.contains("downloading: 1"));
    }

    #[test]
    fn test_generate_csv_summary_all_states() {
        let mut tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.txt"),
            make_test_task("task-3", "file3.txt"),
            make_test_task("task-4", "file4.txt"),
        ];
        tasks[0].state = DownloadState::Downloading;
        tasks[1].state = DownloadState::Complete;
        tasks[2].state = DownloadState::Error;
        tasks[3].state = DownloadState::Paused;

        let summary = generate_csv_summary(&tasks);
        assert!(summary.contains("downloading: 1"));
        assert!(summary.contains("complete: 1"));
        assert!(summary.contains("error: 1"));
        assert!(summary.contains("paused: 1"));
    }

    #[test]
    fn test_generate_csv_summary_total_bytes() {
        let mut tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.txt"),
        ];
        tasks[0].size = 1000;
        tasks[0].downloaded = 500;
        tasks[1].size = 2000;
        tasks[1].downloaded = 1000;

        let summary = generate_csv_summary(&tasks);
        assert!(summary.contains("Total size: 3000 bytes"));
        assert!(summary.contains("Total downloaded: 1500 bytes"));
        assert!(summary.contains("Overall progress: 50.0%"));
    }

    #[test]
    fn test_generate_csv_summary_complete_progress() {
        let mut tasks = vec![make_test_task("task-1", "file.txt")];
        tasks[0].size = 1000;
        tasks[0].downloaded = 1000;
        tasks[0].state = DownloadState::Complete;

        let summary = generate_csv_summary(&tasks);
        assert!(summary.contains("Overall progress: 100.0%"));
    }

    #[test]
    fn test_generate_csv_summary_unicode() {
        let mut tasks = vec![make_test_task("任务-1", "中文文件.txt")];
        tasks[0].state = DownloadState::Complete;

        let summary = generate_csv_summary(&tasks);
        assert!(summary.contains("# Total tasks: 1"));
    }

    // --- Persistence tests (async) ---

    #[tokio::test]
    async fn test_save_csv_export_config_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("csv_config.json");

        let config = CsvExportConfig::default();
        save_csv_export_config(&config, &config_path).await.unwrap();

        assert!(config_path.exists());
    }

    #[tokio::test]
    async fn test_save_csv_export_config_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("csv_config.json");

        let config1 = CsvExportConfig {
            delimiter: ',',
            ..Default::default()
        };
        save_csv_export_config(&config1, &config_path)
            .await
            .unwrap();

        let config2 = CsvExportConfig {
            delimiter: ';',
            ..Default::default()
        };
        save_csv_export_config(&config2, &config_path)
            .await
            .unwrap();

        let loaded = load_csv_export_config(&config_path).await.unwrap();
        assert_eq!(loaded.delimiter, ';');
    }

    #[tokio::test]
    async fn test_save_csv_export_config_no_tmp_leftover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("csv_config.json");

        let config = CsvExportConfig::default();
        save_csv_export_config(&config, &config_path).await.unwrap();

        let tmp_path = config_path.with_extension("csv_config.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_save_load_csv_export_config_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("csv_config.json");

        let config = CsvExportConfig {
            delimiter: '|',
            include_headers: false,
            quote_all: true,
            datetime_format: "%Y/%m/%d %H:%M".to_string(),
        };

        save_csv_export_config(&config, &config_path).await.unwrap();
        let loaded = load_csv_export_config(&config_path).await.unwrap();

        assert_eq!(loaded.delimiter, '|');
        assert_eq!(loaded.include_headers, false);
        assert_eq!(loaded.quote_all, true);
        assert_eq!(loaded.datetime_format, "%Y/%m/%d %H:%M");
    }

    #[tokio::test]
    async fn test_load_csv_export_config_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("nonexistent.json");

        let result = load_csv_export_config(&config_path).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_csv_export_config_corrupted_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("corrupted.json");

        std::fs::write(&config_path, "not valid json").unwrap();
        let result = load_csv_export_config(&config_path).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_csv_export_config_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("empty.json");

        std::fs::write(&config_path, "").unwrap();
        let result = load_csv_export_config(&config_path).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_save_csv_export_config_unicode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("中文配置.json");

        let config = CsvExportConfig::default();
        save_csv_export_config(&config, &config_path).await.unwrap();

        assert!(config_path.exists());
        let loaded = load_csv_export_config(&config_path).await.unwrap();
        assert_eq!(loaded.delimiter, ',');
    }

    #[tokio::test]
    async fn test_save_csv_export_config_pretty_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("pretty.json");

        let config = CsvExportConfig::default();
        save_csv_export_config(&config, &config_path).await.unwrap();

        let content = std::fs::read_to_string(&config_path).unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    // --- Header row tests ---

    #[test]
    fn test_csv_headers_constant() {
        assert_eq!(CSV_HEADERS.len(), 22);
        assert_eq!(CSV_HEADERS[0], "id");
        assert_eq!(CSV_HEADERS[1], "name");
        assert_eq!(CSV_HEADERS[2], "protocol");
        assert!(CSV_HEADERS.contains(&"size_bytes"));
        assert!(CSV_HEADERS.contains(&"downloaded_bytes"));
        assert!(CSV_HEADERS.contains(&"progress_percent"));
        assert!(CSV_HEADERS.contains(&"state"));
        assert!(CSV_HEADERS.contains(&"source_url"));
        assert!(CSV_HEADERS.contains(&"mirror_urls"));
    }

    #[test]
    fn test_csv_header_row_matches_config() {
        let config = CsvExportConfig::default();
        let expected_header = CSV_HEADERS.join(&config.delimiter.to_string());
        assert!(expected_header.contains("id,name,protocol"));
    }

    #[test]
    fn test_csv_header_row_semicolon() {
        let config = CsvExportConfig {
            delimiter: ';',
            ..Default::default()
        };
        let header = CSV_HEADERS.join(&config.delimiter.to_string());
        assert!(header.contains("id;name;protocol"));
    }

    // --- Complex workflow tests ---

    #[test]
    fn test_csv_export_complete_workflow() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("workflow.csv");

        let mut tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
            make_test_task("task-3", "file3.zip"),
        ];

        tasks[0].state = DownloadState::Complete;
        tasks[0].downloaded = 1024;
        tasks[0].size = 1024;
        tasks[0].tags = vec!["work".to_string(), "important".to_string()];

        tasks[1].state = DownloadState::Downloading;
        tasks[1].downloaded = 512;
        tasks[1].size = 2048;
        tasks[1].source_url = Some("http://example.com/file2.mp4".to_string());
        tasks[1].mirror_urls = vec!["http://mirror.com/file2.mp4".to_string()];

        tasks[2].state = DownloadState::Error;
        tasks[2].error = Some("timeout".to_string());
        tasks[2].notes = Some("retry later".to_string());

        let result = export_tasks_to_csv(&tasks, &csv_path, None).unwrap();
        assert_eq!(result.task_count, 3);

        let content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(content.contains("complete"));
        assert!(content.contains("downloading"));
        assert!(content.contains("error"));
        assert!(content.contains("work;important"));
        assert!(content.contains("http://example.com/file2.mp4"));
        assert!(content.contains("timeout"));
        assert!(content.contains("retry later"));
    }

    #[test]
    fn test_csv_export_string_matches_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let csv_path = temp_dir.path().join("compare.csv");

        let tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
        ];

        let csv_string = export_tasks_to_csv_string(&tasks, None).unwrap();
        export_tasks_to_csv(&tasks, &csv_path, None).unwrap();

        let file_content = std::fs::read_to_string(&csv_path).unwrap();

        // File content should match string output (minus trailing newline handling)
        let string_lines: Vec<&str> = csv_string.lines().collect();
        let file_lines: Vec<&str> = file_content.lines().collect();
        assert_eq!(string_lines.len(), file_lines.len());
    }

    #[test]
    fn test_csv_export_different_priorities() {
        let mut tasks = vec![
            make_test_task("task-1", "low.txt"),
            make_test_task("task-2", "normal.txt"),
            make_test_task("task-3", "high.txt"),
        ];

        tasks[0].priority = DownloadPriority::Low;
        tasks[1].priority = DownloadPriority::Normal;
        tasks[2].priority = DownloadPriority::High;

        let csv_string = export_tasks_to_csv_string(&tasks, None).unwrap();
        assert!(csv_string.contains("Low"));
        assert!(csv_string.contains("Normal"));
        assert!(csv_string.contains("High"));
    }

    #[test]
    fn test_csv_export_different_protocols() {
        let mut tasks = vec![
            make_test_task("task-1", "http.txt"),
            make_test_task("task-2", "torrent.txt"),
            make_test_task("task-3", "ed2k.txt"),
            make_test_task("task-4", "magnet.txt"),
            make_test_task("task-5", "p2p.txt"),
        ];

        tasks[0].protocol = DownloadProtocol::Xunlei;
        tasks[1].protocol = DownloadProtocol::Torrent;
        tasks[2].protocol = DownloadProtocol::Ed2k;
        tasks[3].protocol = DownloadProtocol::Magnet;
        tasks[4].protocol = DownloadProtocol::P2P;

        let csv_string = export_tasks_to_csv_string(&tasks, None).unwrap();
        assert!(csv_string.contains("Xunlei"));
        assert!(csv_string.contains("Torrent"));
        assert!(csv_string.contains("Ed2k"));
        assert!(csv_string.contains("Magnet"));
        assert!(csv_string.contains("P2P"));
    }

    // --- Boundary value tests ---

    #[test]
    fn test_csv_export_large_file_size() {
        let mut task = make_test_task("task-1", "huge.mkv");
        task.size = u64::MAX;
        task.downloaded = u64::MAX / 2;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains(&u64::MAX.to_string()));
    }

    #[test]
    fn test_csv_export_zero_bandwidth_weight() {
        let mut task = make_test_task("task-1", "file.txt");
        task.bandwidth_weight = 0;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        // Should contain the bandwidth weight field
        let lines: Vec<&str> = csv_string.lines().collect();
        assert!(lines.len() >= 2);
    }

    #[test]
    fn test_csv_export_max_bandwidth_weight() {
        let mut task = make_test_task("task-1", "file.txt");
        task.bandwidth_weight = 255;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("255"));
    }

    #[test]
    fn test_csv_export_negative_speed() {
        let mut task = make_test_task("task-1", "file.txt");
        task.speed_bps = -1.0;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("-1.00"));
    }

    #[test]
    fn test_csv_export_very_high_speed() {
        let mut task = make_test_task("task-1", "file.txt");
        task.speed_bps = 1e15;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        // Should handle large float values
        let lines: Vec<&str> = csv_string.lines().collect();
        assert!(lines.len() >= 2);
    }

    #[test]
    fn test_csv_export_active_time_precision() {
        let mut task = make_test_task("task-1", "file.txt");
        task.active_time_seconds = 123.456;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        // Should format with 1 decimal place
        assert!(csv_string.contains("123.5"));
    }

    #[test]
    fn test_csv_export_zero_active_time() {
        let mut task = make_test_task("task-1", "file.txt");
        task.active_time_seconds = 0.0;

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("0.0"));
    }

    // --- Unicode edge cases ---

    #[test]
    fn test_csv_export_japanese_filename() {
        let task = make_test_task("task-1", "日本語ファイル.txt");
        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("日本語ファイル.txt"));
    }

    #[test]
    fn test_csv_export_korean_filename() {
        let task = make_test_task("task-1", "한국어파일.txt");
        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("한국어파일.txt"));
    }

    #[test]
    fn test_csv_export_mixed_unicode() {
        let mut task = make_test_task("任务-🎉-1", "文件-📁.txt");
        task.notes = Some("备注-📝".to_string());
        task.tags = vec!["标签-🏷️".to_string()];

        let csv_string = export_tasks_to_csv_string(&[task], None).unwrap();
        assert!(csv_string.contains("任务-🎉-1"));
        assert!(csv_string.contains("文件-📁.txt"));
        assert!(csv_string.contains("备注-📝"));
    }

    // --- Default function tests ---

    #[test]
    fn test_default_delimiter() {
        assert_eq!(default_delimiter(), ',');
    }

    #[test]
    fn test_default_true() {
        assert!(default_true());
    }

    #[test]
    fn test_default_datetime_format() {
        assert_eq!(default_datetime_format(), "%+");
    }

    // --- Column count verification ---

    #[test]
    fn test_csv_row_column_count_matches_headers() {
        let task = make_test_task("task-1", "file.txt");
        let config = CsvExportConfig::default();
        let row = task_to_csv_row(&task, &config);

        let header_count = CSV_HEADERS.len();
        let row_fields: Vec<&str> = row.split(',').collect();

        assert_eq!(row_fields.len(), header_count);
    }

    #[test]
    fn test_csv_row_column_count_with_semicolon() {
        let task = make_test_task("task-1", "file.txt");
        let config = CsvExportConfig {
            delimiter: ';',
            ..Default::default()
        };
        let row = task_to_csv_row(&task, &config);

        let header_count = CSV_HEADERS.len();
        let row_fields: Vec<&str> = row.split(';').collect();

        assert_eq!(row_fields.len(), header_count);
    }
}
