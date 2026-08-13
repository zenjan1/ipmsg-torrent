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
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
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
}
