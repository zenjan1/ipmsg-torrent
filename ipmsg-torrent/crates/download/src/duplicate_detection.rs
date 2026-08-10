//! Download Duplicate Detection System
//!
//! Detects duplicate download tasks based on:
//! - Exact URL match
//! - Similar filename (Levenshtein distance)
//! - Same checksum (if available)
//! - Same file size (if available)
//!
//! Provides deduplication suggestions and automatic merging.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Duplicate detection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateDetectionConfig {
    /// Enable automatic duplicate detection
    pub enabled: bool,
    /// Enable URL-based detection
    pub detect_by_url: bool,
    /// Enable filename-based detection
    pub detect_by_filename: bool,
    /// Enable checksum-based detection
    pub detect_by_checksum: bool,
    /// Enable file size-based detection
    pub detect_by_size: bool,
    /// Minimum filename similarity (0.0-1.0) for detection
    pub filename_similarity_threshold: f64,
    /// Auto-pause duplicates when detected
    pub auto_pause_duplicates: bool,
    /// Auto-remove duplicates when detected
    pub auto_remove_duplicates: bool,
    /// Maximum duplicate groups to track
    pub max_duplicate_groups: usize,
}

impl Default for DuplicateDetectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            detect_by_url: true,
            detect_by_filename: true,
            detect_by_checksum: true,
            detect_by_size: true,
            filename_similarity_threshold: 0.85,
            auto_pause_duplicates: false,
            auto_remove_duplicates: false,
            max_duplicate_groups: 100,
        }
    }
}

/// Duplicate detection method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DuplicateMethod {
    /// Exact URL match
    Url,
    /// Similar filename
    Filename,
    /// Same checksum
    Checksum,
    /// Same file size
    FileSize,
}

impl std::fmt::Display for DuplicateMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuplicateMethod::Url => write!(f, "URL"),
            DuplicateMethod::Filename => write!(f, "Filename"),
            DuplicateMethod::Checksum => write!(f, "Checksum"),
            DuplicateMethod::FileSize => write!(f, "FileSize"),
        }
    }
}

/// Duplicate severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DuplicateSeverity {
    /// Likely duplicate (same size or similar name)
    Low,
    /// Probable duplicate (same checksum)
    Medium,
    /// Confirmed duplicate (same URL)
    High,
}

impl std::fmt::Display for DuplicateSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuplicateSeverity::Low => write!(f, "Low"),
            DuplicateSeverity::Medium => write!(f, "Medium"),
            DuplicateSeverity::High => write!(f, "High"),
        }
    }
}

/// A detected duplicate task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateTask {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Task URL
    pub url: String,
    /// Detection method
    pub method: DuplicateMethod,
    /// Similarity score (0.0-1.0)
    pub similarity: f64,
    /// Severity level
    pub severity: DuplicateSeverity,
    /// Detection timestamp
    pub detected_at: SystemTime,
}

/// A group of duplicate tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup {
    /// Group ID
    pub group_id: String,
    /// Tasks in this group
    pub tasks: Vec<DuplicateTask>,
    /// Primary task (oldest or highest priority)
    pub primary_task_id: String,
    /// Detection method
    pub method: DuplicateMethod,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last updated timestamp
    pub updated_at: SystemTime,
}

/// Task data for duplicate detection
#[derive(Debug, Clone)]
pub struct TaskDuplicateData {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Task URL
    pub url: String,
    /// Expected checksum (if known)
    pub checksum: Option<String>,
    /// Expected file size (if known)
    pub file_size: Option<u64>,
    /// Task state
    pub state: String,
    /// Task priority
    pub priority: i32,
    /// Creation timestamp
    pub created_at: SystemTime,
}

/// Duplicate detection summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateSummary {
    /// Total duplicate groups
    pub total_groups: usize,
    /// Total duplicate tasks
    pub total_duplicates: usize,
    /// Duplicates by method
    pub by_method: HashMap<String, usize>,
    /// Duplicates by severity
    pub by_severity: HashMap<String, usize>,
    /// Estimated wasted space (bytes)
    pub estimated_wasted_bytes: u64,
    /// Last scan timestamp
    pub last_scan_at: Option<SystemTime>,
}

/// Duplicate detection manager
pub struct DuplicateDetectionManager {
    config: DuplicateDetectionConfig,
    duplicate_groups: HashMap<String, DuplicateGroup>,
    task_to_group: HashMap<String, String>,
    last_scan_at: Option<SystemTime>,
}

impl DuplicateDetectionManager {
    /// Create a new duplicate detection manager
    pub fn new() -> Self {
        Self {
            config: DuplicateDetectionConfig::default(),
            duplicate_groups: HashMap::new(),
            task_to_group: HashMap::new(),
            last_scan_at: None,
        }
    }

    /// Create with configuration
    pub fn with_config(config: DuplicateDetectionConfig) -> Self {
        Self {
            config,
            duplicate_groups: HashMap::new(),
            task_to_group: HashMap::new(),
            last_scan_at: None,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &DuplicateDetectionConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: DuplicateDetectionConfig) {
        self.config = config;
    }

    /// Detect duplicates among tasks
    pub fn detect_duplicates(&mut self, tasks: &[TaskDuplicateData]) -> Vec<DuplicateGroup> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut new_groups: Vec<DuplicateGroup> = Vec::new();

        // Clear previous detection
        self.duplicate_groups.clear();
        self.task_to_group.clear();

        // Detect by URL
        if self.config.detect_by_url {
            let url_groups = self.detect_by_url(tasks);
            new_groups.extend(url_groups);
        }

        // Detect by checksum
        if self.config.detect_by_checksum {
            let checksum_groups = self.detect_by_checksum(tasks);
            new_groups.extend(checksum_groups);
        }

        // Detect by file size
        if self.config.detect_by_size {
            let size_groups = self.detect_by_size(tasks);
            new_groups.extend(size_groups);
        }

        // Detect by filename similarity
        if self.config.detect_by_filename {
            let filename_groups = self.detect_by_filename(tasks);
            new_groups.extend(filename_groups);
        }

        // Limit groups
        if new_groups.len() > self.config.max_duplicate_groups {
            new_groups.truncate(self.config.max_duplicate_groups);
        }

        // Store groups
        for group in &new_groups {
            self.duplicate_groups
                .insert(group.group_id.clone(), group.clone());
            for task in &group.tasks {
                self.task_to_group
                    .insert(task.task_id.clone(), group.group_id.clone());
            }
        }

        self.last_scan_at = Some(SystemTime::now());
        new_groups
    }

    /// Detect duplicates by exact URL match
    fn detect_by_url(&self, tasks: &[TaskDuplicateData]) -> Vec<DuplicateGroup> {
        let mut url_map: HashMap<String, Vec<&TaskDuplicateData>> = HashMap::new();

        for task in tasks {
            let normalized_url = self.normalize_url(&task.url);
            url_map.entry(normalized_url).or_default().push(task);
        }

        url_map
            .into_iter()
            .filter(|(_, tasks)| tasks.len() > 1)
            .map(|(url, tasks)| {
                let group_id = format!("url_{}", self.hash_string(&url));
                let duplicate_tasks: Vec<DuplicateTask> = tasks
                    .iter()
                    .map(|t| DuplicateTask {
                        task_id: t.task_id.clone(),
                        task_name: t.task_name.clone(),
                        url: t.url.clone(),
                        method: DuplicateMethod::Url,
                        similarity: 1.0,
                        severity: DuplicateSeverity::High,
                        detected_at: SystemTime::now(),
                    })
                    .collect();

                let primary_task_id = self.select_primary_task(&tasks);

                DuplicateGroup {
                    group_id,
                    tasks: duplicate_tasks,
                    primary_task_id,
                    method: DuplicateMethod::Url,
                    created_at: SystemTime::now(),
                    updated_at: SystemTime::now(),
                }
            })
            .collect()
    }

    /// Detect duplicates by checksum
    fn detect_by_checksum(&self, tasks: &[TaskDuplicateData]) -> Vec<DuplicateGroup> {
        let mut checksum_map: HashMap<String, Vec<&TaskDuplicateData>> = HashMap::new();

        for task in tasks {
            if let Some(ref checksum) = task.checksum {
                checksum_map.entry(checksum.clone()).or_default().push(task);
            }
        }

        checksum_map
            .into_iter()
            .filter(|(_, tasks)| tasks.len() > 1)
            .map(|(checksum, tasks)| {
                let group_id = format!("checksum_{}", self.hash_string(&checksum));
                let duplicate_tasks: Vec<DuplicateTask> = tasks
                    .iter()
                    .map(|t| DuplicateTask {
                        task_id: t.task_id.clone(),
                        task_name: t.task_name.clone(),
                        url: t.url.clone(),
                        method: DuplicateMethod::Checksum,
                        similarity: 1.0,
                        severity: DuplicateSeverity::Medium,
                        detected_at: SystemTime::now(),
                    })
                    .collect();

                let primary_task_id = self.select_primary_task(&tasks);

                DuplicateGroup {
                    group_id,
                    tasks: duplicate_tasks,
                    primary_task_id,
                    method: DuplicateMethod::Checksum,
                    created_at: SystemTime::now(),
                    updated_at: SystemTime::now(),
                }
            })
            .collect()
    }

    /// Detect duplicates by file size
    fn detect_by_size(&self, tasks: &[TaskDuplicateData]) -> Vec<DuplicateGroup> {
        let mut size_map: HashMap<u64, Vec<&TaskDuplicateData>> = HashMap::new();

        for task in tasks {
            if let Some(size) = task.file_size {
                size_map.entry(size).or_default().push(task);
            }
        }

        size_map
            .into_iter()
            .filter(|(_, tasks)| tasks.len() > 1)
            .map(|(size, tasks)| {
                let group_id = format!("size_{}", size);
                let duplicate_tasks: Vec<DuplicateTask> = tasks
                    .iter()
                    .map(|t| DuplicateTask {
                        task_id: t.task_id.clone(),
                        task_name: t.task_name.clone(),
                        url: t.url.clone(),
                        method: DuplicateMethod::FileSize,
                        similarity: 1.0,
                        severity: DuplicateSeverity::Low,
                        detected_at: SystemTime::now(),
                    })
                    .collect();

                let primary_task_id = self.select_primary_task(&tasks);

                DuplicateGroup {
                    group_id,
                    tasks: duplicate_tasks,
                    primary_task_id,
                    method: DuplicateMethod::FileSize,
                    created_at: SystemTime::now(),
                    updated_at: SystemTime::now(),
                }
            })
            .collect()
    }

    /// Detect duplicates by filename similarity
    fn detect_by_filename(&self, tasks: &[TaskDuplicateData]) -> Vec<DuplicateGroup> {
        let mut groups: Vec<DuplicateGroup> = Vec::new();
        let mut processed: std::collections::HashSet<String> = std::collections::HashSet::new();

        for (i, task1) in tasks.iter().enumerate() {
            if processed.contains(&task1.task_id) {
                continue;
            }

            let filename1 = self.extract_filename(&task1.url);
            let mut similar_tasks: Vec<&TaskDuplicateData> = vec![task1];

            for (j, task2) in tasks.iter().enumerate() {
                if i == j || processed.contains(&task2.task_id) {
                    continue;
                }

                let filename2 = self.extract_filename(&task2.url);
                let similarity = self.calculate_similarity(&filename1, &filename2);

                if similarity >= self.config.filename_similarity_threshold {
                    similar_tasks.push(task2);
                }
            }

            if similar_tasks.len() > 1 {
                let group_id = format!("filename_{}", self.hash_string(&filename1));
                let duplicate_tasks: Vec<DuplicateTask> = similar_tasks
                    .iter()
                    .map(|t| {
                        let filename2 = self.extract_filename(&t.url);
                        let similarity = self.calculate_similarity(&filename1, &filename2);
                        DuplicateTask {
                            task_id: t.task_id.clone(),
                            task_name: t.task_name.clone(),
                            url: t.url.clone(),
                            method: DuplicateMethod::Filename,
                            similarity,
                            severity: DuplicateSeverity::Low,
                            detected_at: SystemTime::now(),
                        }
                    })
                    .collect();

                let primary_task_id = self.select_primary_task(&similar_tasks);

                for task in &similar_tasks {
                    processed.insert(task.task_id.clone());
                }

                groups.push(DuplicateGroup {
                    group_id,
                    tasks: duplicate_tasks,
                    primary_task_id,
                    method: DuplicateMethod::Filename,
                    created_at: SystemTime::now(),
                    updated_at: SystemTime::now(),
                });
            }
        }

        groups
    }

    /// Select primary task (oldest or highest priority)
    fn select_primary_task(&self, tasks: &[&TaskDuplicateData]) -> String {
        tasks
            .iter()
            .max_by(|a, b| {
                a.priority
                    .cmp(&b.priority)
                    .then_with(|| b.created_at.cmp(&a.created_at))
            })
            .map(|t| t.task_id.clone())
            .unwrap_or_else(|| tasks[0].task_id.clone())
    }

    /// Normalize URL for comparison
    fn normalize_url(&self, url: &str) -> String {
        url.trim().to_lowercase()
    }

    /// Extract filename from URL
    fn extract_filename(&self, url: &str) -> String {
        url.rsplit('/').next().unwrap_or(url).to_string()
    }

    /// Calculate similarity between two strings (Jaccard similarity)
    fn calculate_similarity(&self, s1: &str, s2: &str) -> f64 {
        if s1 == s2 {
            return 1.0;
        }

        let set1: std::collections::HashSet<_> = s1.chars().collect();
        let set2: std::collections::HashSet<_> = s2.chars().collect();

        let intersection = set1.intersection(&set2).count();
        let union = set1.union(&set2).count();

        if union == 0 {
            return 0.0;
        }

        intersection as f64 / union as f64
    }

    /// Hash string to create unique IDs
    fn hash_string(&self, s: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Get all duplicate groups
    pub fn get_duplicate_groups(&self) -> Vec<&DuplicateGroup> {
        self.duplicate_groups.values().collect()
    }

    /// Get duplicate group by ID
    pub fn get_duplicate_group(&self, group_id: &str) -> Option<&DuplicateGroup> {
        self.duplicate_groups.get(group_id)
    }

    /// Get duplicate group for a task
    pub fn get_task_duplicate_group(&self, task_id: &str) -> Option<&DuplicateGroup> {
        self.task_to_group
            .get(task_id)
            .and_then(|group_id| self.duplicate_groups.get(group_id))
    }

    /// Remove duplicate group
    pub fn remove_duplicate_group(&mut self, group_id: &str) -> bool {
        if let Some(group) = self.duplicate_groups.remove(group_id) {
            for task in &group.tasks {
                self.task_to_group.remove(&task.task_id);
            }
            true
        } else {
            false
        }
    }

    /// Clear all duplicate groups
    pub fn clear_duplicate_groups(&mut self) {
        self.duplicate_groups.clear();
        self.task_to_group.clear();
    }

    /// Get duplicate detection summary
    pub fn get_summary(&self) -> DuplicateSummary {
        let mut by_method: HashMap<String, usize> = HashMap::new();
        let mut by_severity: HashMap<String, usize> = HashMap::new();
        let mut total_duplicates = 0;

        for group in self.duplicate_groups.values() {
            *by_method.entry(group.method.to_string()).or_default() += 1;
            for task in &group.tasks {
                total_duplicates += 1;
                *by_severity.entry(task.severity.to_string()).or_default() += 1;
            }
        }

        DuplicateSummary {
            total_groups: self.duplicate_groups.len(),
            total_duplicates,
            by_method,
            by_severity,
            estimated_wasted_bytes: 0, // Would need file size info
            last_scan_at: self.last_scan_at,
        }
    }

    /// Save configuration to file
    pub fn save_config(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    /// Load configuration from file
    pub fn load_config(path: &std::path::Path) -> Result<DuplicateDetectionConfig, std::io::Error> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(std::io::Error::other)
    }
}

impl Default for DuplicateDetectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_task(id: &str, name: &str, url: &str) -> TaskDuplicateData {
        TaskDuplicateData {
            task_id: id.to_string(),
            task_name: name.to_string(),
            url: url.to_string(),
            checksum: None,
            file_size: None,
            state: "Queued".to_string(),
            priority: 0,
            created_at: SystemTime::now(),
        }
    }

    #[test]
    fn test_detect_duplicates_by_url() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false; // disable filename to isolate URL detection
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "https://example.com/file.zip"),
            create_test_task("3", "Task 3", "https://example.com/other.zip"),
        ];

        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].tasks.len(), 2);
        assert_eq!(groups[0].method, DuplicateMethod::Url);
    }

    #[test]
    fn test_detect_duplicates_by_checksum() {
        let mut manager = DuplicateDetectionManager::new();
        let mut task1 = create_test_task("1", "Task 1", "https://example.com/file1.zip");
        task1.checksum = Some("abc123".to_string());
        let mut task2 = create_test_task("2", "Task 2", "https://example.com/file2.zip");
        task2.checksum = Some("abc123".to_string());

        let tasks = vec![task1, task2];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].method, DuplicateMethod::Checksum);
    }

    #[test]
    fn test_detect_duplicates_by_size() {
        let mut manager = DuplicateDetectionManager::new();
        let mut task1 = create_test_task("1", "Task 1", "https://example.com/file1.zip");
        task1.file_size = Some(1024);
        let mut task2 = create_test_task("2", "Task 2", "https://example.com/file2.zip");
        task2.file_size = Some(1024);

        let tasks = vec![task1, task2];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].method, DuplicateMethod::FileSize);
    }

    #[test]
    fn test_detect_duplicates_by_filename() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.filename_similarity_threshold = 0.7;
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/document.pdf"),
            create_test_task("2", "Task 2", "https://other.com/document.pdf"),
        ];

        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].method, DuplicateMethod::Filename);
    }

    #[test]
    fn test_no_duplicates() {
        let mut manager = DuplicateDetectionManager::new();
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file1.zip"),
            create_test_task("2", "Task 2", "https://example.com/file2.zip"),
        ];

        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_disabled_detection() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.enabled = false;
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "https://example.com/file.zip"),
        ];

        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_get_summary() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false; // disable filename to isolate URL detection
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "https://example.com/file.zip"),
        ];

        manager.detect_duplicates(&tasks);
        let summary = manager.get_summary();
        assert_eq!(summary.total_groups, 1);
        assert_eq!(summary.total_duplicates, 2);
    }

    #[test]
    fn test_remove_duplicate_group() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false; // disable filename to isolate URL detection
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "https://example.com/file.zip"),
        ];

        let groups = manager.detect_duplicates(&tasks);
        let group_id = groups[0].group_id.clone();

        assert!(manager.remove_duplicate_group(&group_id));
        assert_eq!(manager.duplicate_groups.len(), 0);
    }

    #[test]
    fn test_get_task_duplicate_group() {
        let mut manager = DuplicateDetectionManager::new();
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "https://example.com/file.zip"),
        ];

        manager.detect_duplicates(&tasks);
        let group = manager.get_task_duplicate_group("1");
        assert!(group.is_some());
        assert_eq!(group.unwrap().tasks.len(), 2);
    }

    #[test]
    fn test_config_persistence() {
        let manager = DuplicateDetectionManager::new();
        let temp_path = std::env::temp_dir().join("dup_config_test.json");

        manager.save_config(&temp_path).unwrap();
        let loaded = DuplicateDetectionManager::load_config(&temp_path).unwrap();

        assert_eq!(loaded.enabled, manager.config().enabled);
        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_url_normalization() {
        let mut manager = DuplicateDetectionManager::new();
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "HTTPS://EXAMPLE.COM/FILE.ZIP"),
        ];

        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn test_similarity_calculation() {
        let manager = DuplicateDetectionManager::new();
        assert_eq!(manager.calculate_similarity("abc", "abc"), 1.0);
        assert_eq!(manager.calculate_similarity("abc", "def"), 0.0);
        assert!(manager.calculate_similarity("abc", "abd") >= 0.5);
    }

    #[test]
    fn test_extract_filename() {
        let manager = DuplicateDetectionManager::new();
        assert_eq!(
            manager.extract_filename("https://example.com/file.zip"),
            "file.zip"
        );
        assert_eq!(manager.extract_filename("file.zip"), "file.zip");
    }

    #[test]
    fn test_severity_ordering() {
        assert!(DuplicateSeverity::High > DuplicateSeverity::Medium);
        assert!(DuplicateSeverity::Medium > DuplicateSeverity::Low);
    }

    #[test]
    fn test_method_display() {
        assert_eq!(DuplicateMethod::Url.to_string(), "URL");
        assert_eq!(DuplicateMethod::Filename.to_string(), "Filename");
        assert_eq!(DuplicateMethod::Checksum.to_string(), "Checksum");
        assert_eq!(DuplicateMethod::FileSize.to_string(), "FileSize");
    }
}
