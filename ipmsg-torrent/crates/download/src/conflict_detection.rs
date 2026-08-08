//! Download Conflict Detection
//!
//! Detects and resolves file path conflicts before downloads start:
//! - Task-to-task conflicts: two tasks targeting the same save_path
//! - Task-to-disk conflicts: target file already exists on disk
//! - Configurable resolution strategies: skip, rename, overwrite

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Strategy for resolving file path conflicts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConflictStrategy {
    /// Skip the conflicting download (don't add it)
    #[default]
    Skip,
    /// Auto-rename by appending a number suffix (e.g., file(1).txt)
    Rename,
    /// Allow overwrite of existing file
    Overwrite,
}

impl std::fmt::Display for ConflictStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictStrategy::Skip => write!(f, "skip"),
            ConflictStrategy::Rename => write!(f, "rename"),
            ConflictStrategy::Overwrite => write!(f, "overwrite"),
        }
    }
}

impl std::str::FromStr for ConflictStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "skip" => Ok(ConflictStrategy::Skip),
            "rename" => Ok(ConflictStrategy::Rename),
            "overwrite" | "replace" => Ok(ConflictStrategy::Overwrite),
            _ => Err(format!(
                "invalid conflict strategy: {s} (valid: skip, rename, overwrite)"
            )),
        }
    }
}

/// Type of conflict detected
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictType {
    /// Another active/paused task targets the same file path
    TaskConflict {
        /// ID of the conflicting existing task
        existing_task_id: String,
        /// Name of the conflicting existing task
        existing_task_name: String,
    },
    /// The target file already exists on disk (not from another task)
    FileExists {
        /// Size of the existing file on disk
        existing_size: u64,
    },
    /// The target directory doesn't exist
    DirectoryMissing,
}

/// Result of a conflict check for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictReport {
    /// The task ID being checked
    pub task_id: String,
    /// The task name
    pub task_name: String,
    /// The target file path
    pub target_path: PathBuf,
    /// Detected conflict, if any
    pub conflict: Option<ConflictType>,
    /// Resolved path after applying strategy (may differ from target_path if renamed)
    pub resolved_path: PathBuf,
    /// Action taken
    pub action: ConflictAction,
}

/// Action taken to resolve a conflict
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictAction {
    /// No conflict detected, proceed normally
    None,
    /// Task was skipped due to conflict
    Skipped,
    /// Path was auto-renamed to avoid conflict
    Renamed,
    /// Overwrite was allowed
    Overwrite,
}

/// Input data for conflict checking (extracted from DownloadManager)
#[derive(Debug, Clone)]
pub struct TaskPathInfo {
    /// Task ID
    pub id: String,
    /// Task name
    pub name: String,
    /// Target save path (full file path)
    pub save_path: PathBuf,
}

/// Check for conflicts between a new task and existing tasks
pub fn check_task_conflict(
    new_task: &TaskPathInfo,
    existing_tasks: &[TaskPathInfo],
) -> Option<ConflictType> {
    for existing in existing_tasks {
        if existing.id != new_task.id && existing.save_path == new_task.save_path {
            return Some(ConflictType::TaskConflict {
                existing_task_id: existing.id.clone(),
                existing_task_name: existing.name.clone(),
            });
        }
    }
    None
}

/// Check if a file already exists on disk
pub fn check_file_exists_on_disk(path: &Path) -> Option<ConflictType> {
    if let Ok(metadata) = std::fs::metadata(path) {
        if metadata.is_file() {
            return Some(ConflictType::FileExists {
                existing_size: metadata.len(),
            });
        }
    }
    None
}

/// Check if the parent directory exists
pub fn check_directory_exists(path: &Path) -> bool {
    path.parent().map(|p| p.is_dir()).unwrap_or(false)
}

/// Generate a non-conflicting path by appending a number suffix
///
/// Examples:
/// - `file.txt` → `file(1).txt` → `file(2).txt` → ...
/// - `archive.tar.gz` → `archive(1).tar.gz` → ...
pub fn generate_unique_path(path: &Path, existing_paths: &[PathBuf]) -> PathBuf {
    let path_buf = path.to_path_buf();
    if !existing_paths.contains(&path_buf) && !path.exists() {
        return path_buf;
    }

    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
    let extension = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    let parent = path.parent().unwrap_or_else(|| Path::new("."));

    // Handle double extensions like .tar.gz
    let (base_stem, extra_ext) =
        if stem.to_lowercase().ends_with(".tar") && extension.eq_ignore_ascii_case("gz") {
            let inner = stem.strip_suffix(".tar").unwrap_or(stem);
            (inner, ".tar")
        } else {
            (stem, "")
        };

    for i in 1u32..=9999 {
        let new_name = if extra_ext.is_empty() {
            if extension.is_empty() {
                format!("{base_stem}({i})")
            } else {
                format!("{base_stem}({i}).{extension}")
            }
        } else {
            format!("{base_stem}({i}){extra_ext}.{extension}")
        };
        let candidate = parent.join(&new_name);
        if !existing_paths.contains(&candidate) && !candidate.exists() {
            return candidate;
        }
    }

    // Fallback: append timestamp
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let fallback = if extra_ext.is_empty() {
        if extension.is_empty() {
            format!("{base_stem}_{timestamp}")
        } else {
            format!("{base_stem}_{timestamp}.{extension}")
        }
    } else {
        format!("{base_stem}_{timestamp}{extra_ext}.{extension}")
    };
    parent.join(fallback)
}

/// Full conflict check: checks against existing tasks AND disk
pub fn detect_conflicts(
    new_task: &TaskPathInfo,
    existing_tasks: &[TaskPathInfo],
    check_disk: bool,
) -> ConflictReport {
    // 1. Check task-to-task conflict
    if let Some(conflict) = check_task_conflict(new_task, existing_tasks) {
        return ConflictReport {
            task_id: new_task.id.clone(),
            task_name: new_task.name.clone(),
            target_path: new_task.save_path.clone(),
            conflict: Some(conflict),
            resolved_path: new_task.save_path.clone(),
            action: ConflictAction::Skipped,
        };
    }

    // 2. Check disk conflict
    if check_disk {
        if let Some(conflict) = check_file_exists_on_disk(&new_task.save_path) {
            return ConflictReport {
                task_id: new_task.id.clone(),
                task_name: new_task.name.clone(),
                target_path: new_task.save_path.clone(),
                conflict: Some(conflict),
                resolved_path: new_task.save_path.clone(),
                action: ConflictAction::Skipped,
            };
        }
    }

    // No conflict
    ConflictReport {
        task_id: new_task.id.clone(),
        task_name: new_task.name.clone(),
        target_path: new_task.save_path.clone(),
        conflict: None,
        resolved_path: new_task.save_path.clone(),
        action: ConflictAction::None,
    }
}

/// Resolve a conflict using the configured strategy
pub fn resolve_conflict(
    report: &mut ConflictReport,
    strategy: ConflictStrategy,
    existing_paths: &[PathBuf],
) {
    if report.conflict.is_none() {
        return;
    }

    match strategy {
        ConflictStrategy::Skip => {
            report.action = ConflictAction::Skipped;
        }
        ConflictStrategy::Rename => {
            let unique = generate_unique_path(&report.target_path, existing_paths);
            report.resolved_path = unique;
            report.action = ConflictAction::Renamed;
        }
        ConflictStrategy::Overwrite => {
            report.action = ConflictAction::Overwrite;
            report.resolved_path = report.target_path.clone();
        }
    }
}

/// Batch check conflicts for multiple new tasks
pub fn batch_detect_conflicts(
    new_tasks: &[TaskPathInfo],
    existing_tasks: &[TaskPathInfo],
    check_disk: bool,
) -> Vec<ConflictReport> {
    let mut reports: Vec<ConflictReport> = Vec::new();
    let mut accepted_paths: Vec<PathBuf> =
        existing_tasks.iter().map(|t| t.save_path.clone()).collect();

    for (idx, task) in new_tasks.iter().enumerate() {
        let mut report = detect_conflicts(task, existing_tasks, check_disk);

        // Check against earlier tasks in this batch that were accepted
        if report.conflict.is_none() {
            for prev_idx in 0..idx {
                let prev = &new_tasks[prev_idx];
                if prev.save_path == task.save_path && reports[prev_idx].conflict.is_none() {
                    report.conflict = Some(ConflictType::TaskConflict {
                        existing_task_id: prev.id.clone(),
                        existing_task_name: prev.name.clone(),
                    });
                    report.action = ConflictAction::Skipped;
                    break;
                }
            }
        }

        if report.conflict.is_none() {
            accepted_paths.push(task.save_path.clone());
        }

        reports.push(report);
    }

    reports
}

/// Format a conflict report for display
pub fn format_conflict_report(report: &ConflictReport) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Task: {} ({})", report.task_name, report.task_id));
    lines.push(format!("Target: {}", report.target_path.display()));

    match &report.action {
        ConflictAction::None => {
            lines.push("Status: ✅ No conflict".to_string());
        }
        ConflictAction::Skipped => {
            lines.push("Status: ⚠️ Conflict detected, SKIPPED".to_string());
            if let Some(ref conflict) = report.conflict {
                match conflict {
                    ConflictType::TaskConflict {
                        existing_task_id,
                        existing_task_name,
                    } => {
                        lines.push(format!(
                            "  Conflicts with task: {} ({})",
                            existing_task_name, existing_task_id
                        ));
                    }
                    ConflictType::FileExists { existing_size } => {
                        lines.push(format!(
                            "  File already exists on disk ({} bytes)",
                            existing_size
                        ));
                    }
                    ConflictType::DirectoryMissing => {
                        lines.push("  Target directory does not exist".to_string());
                    }
                }
            }
        }
        ConflictAction::Renamed => {
            lines.push("Status: 🔄 Renamed to avoid conflict".to_string());
            lines.push(format!("  New path: {}", report.resolved_path.display()));
        }
        ConflictAction::Overwrite => {
            lines.push("Status: ⚡ Overwrite allowed".to_string());
        }
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn make_task(id: &str, name: &str, path: PathBuf) -> TaskPathInfo {
        TaskPathInfo {
            id: id.to_string(),
            name: name.to_string(),
            save_path: path,
        }
    }

    #[test]
    fn test_conflict_strategy_display() {
        assert_eq!(ConflictStrategy::Skip.to_string(), "skip");
        assert_eq!(ConflictStrategy::Rename.to_string(), "rename");
        assert_eq!(ConflictStrategy::Overwrite.to_string(), "overwrite");
    }

    #[test]
    fn test_conflict_strategy_from_str() {
        assert_eq!(
            "skip".parse::<ConflictStrategy>().unwrap(),
            ConflictStrategy::Skip
        );
        assert_eq!(
            "rename".parse::<ConflictStrategy>().unwrap(),
            ConflictStrategy::Rename
        );
        assert_eq!(
            "overwrite".parse::<ConflictStrategy>().unwrap(),
            ConflictStrategy::Overwrite
        );
        assert_eq!(
            "replace".parse::<ConflictStrategy>().unwrap(),
            ConflictStrategy::Overwrite
        );
        assert!("invalid".parse::<ConflictStrategy>().is_err());
    }

    #[test]
    fn test_no_conflict_different_paths() {
        let task1 = make_task("1", "file1", PathBuf::from("/tmp/file1.txt"));
        let task2 = make_task("2", "file2", PathBuf::from("/tmp/file2.txt"));
        assert!(check_task_conflict(&task1, &[task2.clone()]).is_none());
    }

    #[test]
    fn test_task_conflict_same_path() {
        let task1 = make_task("1", "file1", PathBuf::from("/tmp/same.txt"));
        let task2 = make_task("2", "file2", PathBuf::from("/tmp/same.txt"));
        let conflict = check_task_conflict(&task1, &[task2]).unwrap();
        match conflict {
            ConflictType::TaskConflict {
                existing_task_id,
                existing_task_name,
            } => {
                assert_eq!(existing_task_id, "2");
                assert_eq!(existing_task_name, "file2");
            }
            _ => panic!("expected TaskConflict"),
        }
    }

    #[test]
    fn test_no_self_conflict() {
        let task = make_task("1", "file1", PathBuf::from("/tmp/same.txt"));
        assert!(check_task_conflict(&task, &[task.clone()]).is_none());
    }

    #[test]
    fn test_file_exists_on_disk() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("existing.txt");
        fs::write(&file_path, "hello").unwrap();

        let conflict = check_file_exists_on_disk(&file_path).unwrap();
        match conflict {
            ConflictType::FileExists { existing_size } => {
                assert_eq!(existing_size, 5);
            }
            _ => panic!("expected FileExists"),
        }
    }

    #[test]
    fn test_file_not_exists_on_disk() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("nonexistent.txt");
        assert!(check_file_exists_on_disk(&file_path).is_none());
    }

    #[test]
    fn test_generate_unique_path_no_conflict() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_file.txt");
        let result = generate_unique_path(&path, &[]);
        assert_eq!(result, path);
    }

    #[test]
    fn test_generate_unique_path_file_exists() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("existing.txt");
        fs::write(&path, "data").unwrap();

        let result = generate_unique_path(&path, &[]);
        assert_eq!(result, dir.path().join("existing(1).txt"));
    }

    #[test]
    fn test_generate_unique_path_multiple() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file.txt");
        fs::write(&path, "data").unwrap();
        fs::write(dir.path().join("file(1).txt"), "data").unwrap();
        fs::write(dir.path().join("file(2).txt"), "data").unwrap();

        let result = generate_unique_path(&path, &[]);
        assert_eq!(result, dir.path().join("file(3).txt"));
    }

    #[test]
    fn test_generate_unique_path_in_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file.txt");
        let existing = vec![dir.path().join("file.txt"), dir.path().join("file(1).txt")];

        let result = generate_unique_path(&path, &existing);
        assert_eq!(result, dir.path().join("file(2).txt"));
    }

    #[test]
    fn test_generate_unique_path_tar_gz() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("archive.tar.gz");
        fs::write(&path, "data").unwrap();

        let result = generate_unique_path(&path, &[]);
        assert_eq!(result, dir.path().join("archive(1).tar.gz"));
    }

    #[test]
    fn test_generate_unique_path_no_extension() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("README");
        fs::write(&path, "data").unwrap();

        let result = generate_unique_path(&path, &[]);
        assert_eq!(result, dir.path().join("README(1)"));
    }

    #[test]
    fn test_detect_conflicts_no_conflict() {
        let dir = TempDir::new().unwrap();
        let new = make_task("1", "new", dir.path().join("new.txt"));
        let existing = make_task("2", "existing", dir.path().join("existing.txt"));

        let report = detect_conflicts(&new, &[existing], false);
        assert!(report.conflict.is_none());
        assert_eq!(report.action, ConflictAction::None);
    }

    #[test]
    fn test_detect_conflicts_task_conflict() {
        let new = make_task("1", "new", PathBuf::from("/tmp/same.txt"));
        let existing = make_task("2", "existing", PathBuf::from("/tmp/same.txt"));

        let report = detect_conflicts(&new, &[existing], false);
        assert!(report.conflict.is_some());
        assert_eq!(report.action, ConflictAction::Skipped);
    }

    #[test]
    fn test_detect_conflicts_disk_conflict() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("existing.txt");
        fs::write(&file_path, "data").unwrap();

        let new = make_task("1", "new", file_path.clone());
        let report = detect_conflicts(&new, &[], true);
        assert!(report.conflict.is_some());
        assert_eq!(report.action, ConflictAction::Skipped);
    }

    #[test]
    fn test_resolve_skip() {
        let new = make_task("1", "new", PathBuf::from("/tmp/same.txt"));
        let existing = make_task("2", "existing", PathBuf::from("/tmp/same.txt"));
        let mut report = detect_conflicts(&new, &[existing], false);

        resolve_conflict(&mut report, ConflictStrategy::Skip, &[]);
        assert_eq!(report.action, ConflictAction::Skipped);
    }

    #[test]
    fn test_resolve_rename() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("file.txt");
        fs::write(&file_path, "data").unwrap();

        let new = make_task("1", "new", file_path.clone());
        let mut report = detect_conflicts(&new, &[], true);
        assert!(report.conflict.is_some());

        resolve_conflict(&mut report, ConflictStrategy::Rename, &[]);
        assert_eq!(report.action, ConflictAction::Renamed);
        assert_eq!(report.resolved_path, dir.path().join("file(1).txt"));
    }

    #[test]
    fn test_resolve_overwrite() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("file.txt");
        fs::write(&file_path, "data").unwrap();

        let new = make_task("1", "new", file_path.clone());
        let mut report = detect_conflicts(&new, &[], true);

        resolve_conflict(&mut report, ConflictStrategy::Overwrite, &[]);
        assert_eq!(report.action, ConflictAction::Overwrite);
        assert_eq!(report.resolved_path, file_path);
    }

    #[test]
    fn test_resolve_no_conflict_is_noop() {
        let dir = TempDir::new().unwrap();
        let new = make_task("1", "new", dir.path().join("new.txt"));
        let mut report = detect_conflicts(&new, &[], false);
        assert_eq!(report.action, ConflictAction::None);

        resolve_conflict(&mut report, ConflictStrategy::Rename, &[]);
        assert_eq!(report.action, ConflictAction::None);
    }

    #[test]
    fn test_batch_detect_no_conflicts() {
        let dir = TempDir::new().unwrap();
        let tasks = vec![
            make_task("1", "a", dir.path().join("a.txt")),
            make_task("2", "b", dir.path().join("b.txt")),
        ];
        let reports = batch_detect_conflicts(&tasks, &[], false);
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|r| r.conflict.is_none()));
    }

    #[test]
    fn test_batch_detect_intra_batch_conflict() {
        let tasks = vec![
            make_task("1", "a", PathBuf::from("/tmp/same.txt")),
            make_task("2", "b", PathBuf::from("/tmp/same.txt")),
        ];
        let reports = batch_detect_conflicts(&tasks, &[], false);
        assert_eq!(reports[0].action, ConflictAction::None);
        assert_eq!(reports[1].action, ConflictAction::Skipped);
    }

    #[test]
    fn test_batch_detect_existing_conflict() {
        let existing = vec![make_task("0", "existing", PathBuf::from("/tmp/a.txt"))];
        let new_tasks = vec![
            make_task("1", "a", PathBuf::from("/tmp/a.txt")),
            make_task("2", "b", PathBuf::from("/tmp/b.txt")),
        ];
        let reports = batch_detect_conflicts(&new_tasks, &existing, false);
        assert_eq!(reports[0].action, ConflictAction::Skipped);
        assert_eq!(reports[1].action, ConflictAction::None);
    }

    #[test]
    fn test_format_conflict_report_no_conflict() {
        let report = ConflictReport {
            task_id: "abc".to_string(),
            task_name: "test_file".to_string(),
            target_path: PathBuf::from("/tmp/test.txt"),
            conflict: None,
            resolved_path: PathBuf::from("/tmp/test.txt"),
            action: ConflictAction::None,
        };
        let formatted = format_conflict_report(&report);
        assert!(formatted.contains("✅ No conflict"));
    }

    #[test]
    fn test_format_conflict_report_task_conflict() {
        let report = ConflictReport {
            task_id: "abc".to_string(),
            task_name: "test_file".to_string(),
            target_path: PathBuf::from("/tmp/test.txt"),
            conflict: Some(ConflictType::TaskConflict {
                existing_task_id: "xyz".to_string(),
                existing_task_name: "other_file".to_string(),
            }),
            resolved_path: PathBuf::from("/tmp/test.txt"),
            action: ConflictAction::Skipped,
        };
        let formatted = format_conflict_report(&report);
        assert!(formatted.contains("⚠️ Conflict detected, SKIPPED"));
        assert!(formatted.contains("other_file"));
    }

    #[test]
    fn test_format_conflict_report_renamed() {
        let report = ConflictReport {
            task_id: "abc".to_string(),
            task_name: "test_file".to_string(),
            target_path: PathBuf::from("/tmp/test.txt"),
            conflict: Some(ConflictType::FileExists { existing_size: 100 }),
            resolved_path: PathBuf::from("/tmp/test(1).txt"),
            action: ConflictAction::Renamed,
        };
        let formatted = format_conflict_report(&report);
        assert!(formatted.contains("🔄 Renamed"));
        assert!(formatted.contains("test(1).txt"));
    }

    #[test]
    fn test_check_directory_exists() {
        let dir = TempDir::new().unwrap();
        let existing_file = dir.path().join("file.txt");
        assert!(check_directory_exists(&existing_file));

        let nonexistent = PathBuf::from("/nonexistent_dir_12345/file.txt");
        assert!(!check_directory_exists(&nonexistent));
    }

    #[test]
    fn test_conflict_type_serialization() {
        let conflict = ConflictType::TaskConflict {
            existing_task_id: "123".to_string(),
            existing_task_name: "test".to_string(),
        };
        let json = serde_json::to_string(&conflict).unwrap();
        let deserialized: ConflictType = serde_json::from_str(&json).unwrap();
        assert_eq!(conflict, deserialized);
    }

    #[test]
    fn test_conflict_report_serialization() {
        let report = ConflictReport {
            task_id: "abc".to_string(),
            task_name: "test".to_string(),
            target_path: PathBuf::from("/tmp/test.txt"),
            conflict: None,
            resolved_path: PathBuf::from("/tmp/test.txt"),
            action: ConflictAction::None,
        };
        let json = serde_json::to_string(&report).unwrap();
        let deserialized: ConflictReport = serde_json::from_str(&json).unwrap();
        assert_eq!(report.task_id, deserialized.task_id);
        assert_eq!(report.action, deserialized.action);
    }
}
