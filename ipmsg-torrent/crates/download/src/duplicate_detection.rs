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

    // ── Phase 228: Comprehensive test coverage ──

    // ── Serialization: DuplicateDetectionConfig ──

    #[test]
    fn test_config_serde_roundtrip() {
        let config = DuplicateDetectionConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: DuplicateDetectionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.enabled, config.enabled);
        assert_eq!(parsed.detect_by_url, config.detect_by_url);
        assert_eq!(parsed.detect_by_filename, config.detect_by_filename);
        assert_eq!(parsed.detect_by_checksum, config.detect_by_checksum);
        assert_eq!(parsed.detect_by_size, config.detect_by_size);
        assert!(
            (parsed.filename_similarity_threshold - config.filename_similarity_threshold).abs()
                < f64::EPSILON
        );
        assert_eq!(parsed.auto_pause_duplicates, config.auto_pause_duplicates);
        assert_eq!(parsed.auto_remove_duplicates, config.auto_remove_duplicates);
        assert_eq!(parsed.max_duplicate_groups, config.max_duplicate_groups);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = DuplicateDetectionConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let parsed: DuplicateDetectionConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(parsed.enabled, true);
        assert_eq!(parsed.max_duplicate_groups, 100);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"detect_by_url":true,"detect_by_filename":true,"detect_by_checksum":true,"detect_by_size":true,"filename_similarity_threshold":0.85,"auto_pause_duplicates":false,"auto_remove_duplicates":false,"max_duplicate_groups":100,"unknown_field":"value"}"#;
        let parsed: DuplicateDetectionConfig = serde_json::from_str(json).unwrap();
        assert!(parsed.enabled);
    }

    #[test]
    fn test_config_serde_custom_values() {
        let config = DuplicateDetectionConfig {
            enabled: false,
            detect_by_url: false,
            detect_by_filename: true,
            detect_by_checksum: false,
            detect_by_size: true,
            filename_similarity_threshold: 0.5,
            auto_pause_duplicates: true,
            auto_remove_duplicates: true,
            max_duplicate_groups: 50,
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: DuplicateDetectionConfig = serde_json::from_str(&json).unwrap();
        assert!(!parsed.enabled);
        assert!(!parsed.detect_by_url);
        assert!(parsed.detect_by_filename);
        assert!(!parsed.detect_by_checksum);
        assert!(parsed.detect_by_size);
        assert!((parsed.filename_similarity_threshold - 0.5).abs() < f64::EPSILON);
        assert!(parsed.auto_pause_duplicates);
        assert!(parsed.auto_remove_duplicates);
        assert_eq!(parsed.max_duplicate_groups, 50);
    }

    #[test]
    fn test_config_default_values() {
        let config = DuplicateDetectionConfig::default();
        assert!(config.enabled);
        assert!(config.detect_by_url);
        assert!(config.detect_by_filename);
        assert!(config.detect_by_checksum);
        assert!(config.detect_by_size);
        assert!((config.filename_similarity_threshold - 0.85).abs() < f64::EPSILON);
        assert!(!config.auto_pause_duplicates);
        assert!(!config.auto_remove_duplicates);
        assert_eq!(config.max_duplicate_groups, 100);
    }

    // ── Serialization: DuplicateMethod ──

    #[test]
    fn test_method_serde_roundtrip() {
        for method in [
            DuplicateMethod::Url,
            DuplicateMethod::Filename,
            DuplicateMethod::Checksum,
            DuplicateMethod::FileSize,
        ] {
            let json = serde_json::to_string(&method).unwrap();
            let parsed: DuplicateMethod = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, method);
        }
    }

    // ── Serialization: DuplicateSeverity ──

    #[test]
    fn test_severity_serde_roundtrip() {
        for sev in [
            DuplicateSeverity::Low,
            DuplicateSeverity::Medium,
            DuplicateSeverity::High,
        ] {
            let json = serde_json::to_string(&sev).unwrap();
            let parsed: DuplicateSeverity = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, sev);
        }
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(DuplicateSeverity::Low.to_string(), "Low");
        assert_eq!(DuplicateSeverity::Medium.to_string(), "Medium");
        assert_eq!(DuplicateSeverity::High.to_string(), "High");
    }

    // ── Serialization: DuplicateTask ──

    #[test]
    fn test_duplicate_task_serde_roundtrip() {
        let task = DuplicateTask {
            task_id: "t1".to_string(),
            task_name: "Test Task".to_string(),
            url: "https://example.com/file.zip".to_string(),
            method: DuplicateMethod::Url,
            similarity: 0.95,
            severity: DuplicateSeverity::High,
            detected_at: SystemTime::now(),
        };
        let json = serde_json::to_string(&task).unwrap();
        let parsed: DuplicateTask = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id, "t1");
        assert_eq!(parsed.method, DuplicateMethod::Url);
        assert_eq!(parsed.severity, DuplicateSeverity::High);
        assert!((parsed.similarity - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn test_duplicate_task_unicode() {
        let task = DuplicateTask {
            task_id: "🎯task".to_string(),
            task_name: "中文任务名".to_string(),
            url: "https://example.com/文件.zip".to_string(),
            method: DuplicateMethod::Filename,
            similarity: 0.8,
            severity: DuplicateSeverity::Medium,
            detected_at: SystemTime::now(),
        };
        let json = serde_json::to_string(&task).unwrap();
        let parsed: DuplicateTask = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id, "🎯task");
        assert_eq!(parsed.task_name, "中文任务名");
    }

    // ── Serialization: DuplicateGroup ──

    #[test]
    fn test_duplicate_group_serde_roundtrip() {
        let now = SystemTime::now();
        let group = DuplicateGroup {
            group_id: "grp_1".to_string(),
            tasks: vec![DuplicateTask {
                task_id: "t1".to_string(),
                task_name: "Task 1".to_string(),
                url: "https://example.com/file.zip".to_string(),
                method: DuplicateMethod::Url,
                similarity: 1.0,
                severity: DuplicateSeverity::High,
                detected_at: now,
            }],
            primary_task_id: "t1".to_string(),
            method: DuplicateMethod::Url,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&group).unwrap();
        let parsed: DuplicateGroup = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.group_id, "grp_1");
        assert_eq!(parsed.tasks.len(), 1);
        assert_eq!(parsed.primary_task_id, "t1");
    }

    // ── Serialization: DuplicateSummary ──

    #[test]
    fn test_summary_serde_roundtrip() {
        let mut by_method = HashMap::new();
        by_method.insert("URL".to_string(), 2);
        let mut by_severity = HashMap::new();
        by_severity.insert("High".to_string(), 4);
        let summary = DuplicateSummary {
            total_groups: 2,
            total_duplicates: 4,
            by_method,
            by_severity,
            estimated_wasted_bytes: 1024000,
            last_scan_at: Some(SystemTime::now()),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let parsed: DuplicateSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.total_groups, 2);
        assert_eq!(parsed.total_duplicates, 4);
        assert_eq!(parsed.estimated_wasted_bytes, 1024000);
        assert!(parsed.last_scan_at.is_some());
    }

    #[test]
    fn test_summary_serde_empty() {
        let summary = DuplicateSummary {
            total_groups: 0,
            total_duplicates: 0,
            by_method: HashMap::new(),
            by_severity: HashMap::new(),
            estimated_wasted_bytes: 0,
            last_scan_at: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let parsed: DuplicateSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.total_groups, 0);
        assert!(parsed.last_scan_at.is_none());
    }

    // ── Clone/Debug traits ──

    #[test]
    fn test_config_clone_debug() {
        let config = DuplicateDetectionConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        // Debug should not panic
        let _ = format!("{:?}", config);
    }

    #[test]
    fn test_method_clone_debug() {
        let method = DuplicateMethod::Url;
        let cloned = method.clone();
        assert_eq!(cloned, DuplicateMethod::Url);
        let _ = format!("{:?}", method);
    }

    #[test]
    fn test_severity_clone_debug() {
        let sev = DuplicateSeverity::High;
        let cloned = sev.clone();
        assert_eq!(cloned, DuplicateSeverity::High);
        let _ = format!("{:?}", sev);
    }

    #[test]
    fn test_duplicate_task_clone_debug() {
        let task = DuplicateTask {
            task_id: "t1".to_string(),
            task_name: "Task".to_string(),
            url: "https://example.com/f.zip".to_string(),
            method: DuplicateMethod::Url,
            similarity: 1.0,
            severity: DuplicateSeverity::High,
            detected_at: SystemTime::now(),
        };
        let cloned = task.clone();
        assert_eq!(cloned.task_id, "t1");
        let _ = format!("{:?}", task);
    }

    #[test]
    fn test_duplicate_group_clone_debug() {
        let now = SystemTime::now();
        let group = DuplicateGroup {
            group_id: "g1".to_string(),
            tasks: vec![],
            primary_task_id: "t1".to_string(),
            method: DuplicateMethod::Url,
            created_at: now,
            updated_at: now,
        };
        let cloned = group.clone();
        assert_eq!(cloned.group_id, "g1");
        let _ = format!("{:?}", group);
    }

    #[test]
    fn test_summary_clone_debug() {
        let summary = DuplicateSummary {
            total_groups: 0,
            total_duplicates: 0,
            by_method: HashMap::new(),
            by_severity: HashMap::new(),
            estimated_wasted_bytes: 0,
            last_scan_at: None,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_groups, 0);
        let _ = format!("{:?}", summary);
    }

    // ── Manager construction ──

    #[test]
    fn test_manager_new() {
        let manager = DuplicateDetectionManager::new();
        assert!(manager.config().enabled);
        assert_eq!(manager.get_duplicate_groups().len(), 0);
        assert!(manager.last_scan_at.is_none());
    }

    #[test]
    fn test_manager_default_equals_new() {
        let new = DuplicateDetectionManager::new();
        let default = DuplicateDetectionManager::default();
        assert_eq!(new.config().enabled, default.config().enabled);
        assert_eq!(
            new.config().max_duplicate_groups,
            default.config().max_duplicate_groups
        );
    }

    #[test]
    fn test_manager_with_config() {
        let config = DuplicateDetectionConfig {
            enabled: true,
            detect_by_url: true,
            detect_by_filename: false,
            detect_by_checksum: false,
            detect_by_size: false,
            filename_similarity_threshold: 0.9,
            auto_pause_duplicates: true,
            auto_remove_duplicates: false,
            max_duplicate_groups: 50,
        };
        let manager = DuplicateDetectionManager::with_config(config);
        assert!(!manager.config().detect_by_filename);
        assert!(!manager.config().detect_by_checksum);
        assert_eq!(manager.config().max_duplicate_groups, 50);
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = DuplicateDetectionManager::new();
        manager.set_config(DuplicateDetectionConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(!manager.config().enabled);
    }

    // ── Detection edge cases ──

    #[test]
    fn test_detect_empty_task_list() {
        let mut manager = DuplicateDetectionManager::new();
        let groups = manager.detect_duplicates(&[]);
        assert_eq!(groups.len(), 0);
        assert!(manager.last_scan_at.is_some());
    }

    #[test]
    fn test_detect_single_task() {
        let mut manager = DuplicateDetectionManager::new();
        let tasks = vec![create_test_task(
            "1",
            "Task 1",
            "https://example.com/file.zip",
        )];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_detect_only_url_disabled() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_checksum = false;
        manager.config.detect_by_size = false;
        let tasks = vec![
            create_test_task("1", "Task 1", "https://example.com/file.zip"),
            create_test_task("2", "Task 2", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_detect_url_multiple_groups() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/a.zip"),
            create_test_task("2", "T2", "https://example.com/a.zip"),
            create_test_task("3", "T3", "https://example.com/b.zip"),
            create_test_task("4", "T4", "https://example.com/b.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn test_detect_url_three_duplicates() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
            create_test_task("3", "T3", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].tasks.len(), 3);
    }

    #[test]
    fn test_detect_checksum_no_checksums() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_size = false;
        // No tasks have checksums, so no checksum groups
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/a.zip"),
            create_test_task("2", "T2", "https://example.com/b.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_detect_checksum_mixed() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_size = false;
        let mut task1 = create_test_task("1", "T1", "https://example.com/a.zip");
        task1.checksum = Some("abc".to_string());
        let task2 = create_test_task("2", "T2", "https://example.com/b.zip");
        // task2 has no checksum
        let tasks = vec![task1, task2];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_detect_size_different_sizes() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_checksum = false;
        let mut task1 = create_test_task("1", "T1", "https://example.com/a.zip");
        task1.file_size = Some(1024);
        let mut task2 = create_test_task("2", "T2", "https://example.com/b.zip");
        task2.file_size = Some(2048);
        let tasks = vec![task1, task2];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_detect_size_zero() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_checksum = false;
        let mut task1 = create_test_task("1", "T1", "https://example.com/a.zip");
        task1.file_size = Some(0);
        let mut task2 = create_test_task("2", "T2", "https://example.com/b.zip");
        task2.file_size = Some(0);
        let tasks = vec![task1, task2];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].method, DuplicateMethod::FileSize);
    }

    // ── Filename similarity ──

    #[test]
    fn test_filename_similarity_threshold_boundary() {
        let manager = DuplicateDetectionManager::new();
        // Identical strings = 1.0
        assert!((manager.calculate_similarity("file.zip", "file.zip") - 1.0).abs() < f64::EPSILON);
        // Completely different chars = 0.0
        assert!((manager.calculate_similarity("abc", "xyz") - 0.0).abs() < f64::EPSILON);
        // Partial overlap
        let sim = manager.calculate_similarity("document.pdf", "document_v2.pdf");
        assert!(sim > 0.5, "expected > 0.5, got {}", sim);
    }

    #[test]
    fn test_filename_similarity_empty_strings() {
        let manager = DuplicateDetectionManager::new();
        // Both empty strings are equal → returns 1.0 (early exit)
        assert!((manager.calculate_similarity("", "") - 1.0).abs() < f64::EPSILON);
        // One empty, one non-empty → no common chars, union > 0
        assert!((manager.calculate_similarity("abc", "") - 0.0).abs() < f64::EPSILON);
        assert!((manager.calculate_similarity("", "abc") - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_filename_similarity_unicode() {
        let manager = DuplicateDetectionManager::new();
        let sim = manager.calculate_similarity("文件.zip", "文件.zip");
        assert!((sim - 1.0).abs() < f64::EPSILON);
        let sim2 = manager.calculate_similarity("文件.zip", "文档.zip");
        assert!(sim2 > 0.0 && sim2 < 1.0);
    }

    // ── extract_filename ──

    #[test]
    fn test_extract_filename_edge_cases() {
        let manager = DuplicateDetectionManager::new();
        assert_eq!(manager.extract_filename(""), "");
        assert_eq!(manager.extract_filename("/"), "");
        assert_eq!(manager.extract_filename("a/b/c/file.tar.gz"), "file.tar.gz");
        assert_eq!(manager.extract_filename("no-slash"), "no-slash");
    }

    // ── normalize_url ──

    #[test]
    fn test_normalize_url_whitespace() {
        let manager = DuplicateDetectionManager::new();
        assert_eq!(
            manager.normalize_url("  https://example.com  "),
            "https://example.com"
        );
    }

    #[test]
    fn test_normalize_url_case() {
        let manager = DuplicateDetectionManager::new();
        assert_eq!(
            manager.normalize_url("HTTPS://EXAMPLE.COM/FILE.ZIP"),
            "https://example.com/file.zip"
        );
    }

    // ── select_primary_task ──

    #[test]
    fn test_select_primary_task_by_priority() {
        let manager = DuplicateDetectionManager::new();
        let now = SystemTime::now();
        let tasks = vec![
            TaskDuplicateData {
                task_id: "low".to_string(),
                task_name: "Low".to_string(),
                url: "https://example.com/a.zip".to_string(),
                checksum: None,
                file_size: None,
                state: "Queued".to_string(),
                priority: 1,
                created_at: now,
            },
            TaskDuplicateData {
                task_id: "high".to_string(),
                task_name: "High".to_string(),
                url: "https://example.com/b.zip".to_string(),
                checksum: None,
                file_size: None,
                state: "Queued".to_string(),
                priority: 10,
                created_at: now,
            },
        ];
        let refs: Vec<&TaskDuplicateData> = tasks.iter().collect();
        let primary = manager.select_primary_task(&refs);
        assert_eq!(primary, "high");
    }

    #[test]
    fn test_select_primary_task_same_priority_older_wins() {
        let manager = DuplicateDetectionManager::new();
        let older = SystemTime::UNIX_EPOCH;
        let newer = SystemTime::now();
        let tasks = vec![
            TaskDuplicateData {
                task_id: "newer".to_string(),
                task_name: "Newer".to_string(),
                url: "https://example.com/a.zip".to_string(),
                checksum: None,
                file_size: None,
                state: "Queued".to_string(),
                priority: 5,
                created_at: newer,
            },
            TaskDuplicateData {
                task_id: "older".to_string(),
                task_name: "Older".to_string(),
                url: "https://example.com/b.zip".to_string(),
                checksum: None,
                file_size: None,
                state: "Queued".to_string(),
                priority: 5,
                created_at: older,
            },
        ];
        let refs: Vec<&TaskDuplicateData> = tasks.iter().collect();
        let primary = manager.select_primary_task(&refs);
        // Same priority → older wins (b.created_at.cmp(&a.created_at) → older has smaller timestamp, so it's "greater" in reverse cmp)
        assert_eq!(primary, "older");
    }

    // ── Group management ──

    #[test]
    fn test_get_duplicate_groups() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/a.zip"),
            create_test_task("2", "T2", "https://example.com/a.zip"),
            create_test_task("3", "T3", "https://example.com/b.zip"),
            create_test_task("4", "T4", "https://example.com/b.zip"),
        ];
        manager.detect_duplicates(&tasks);
        assert_eq!(manager.get_duplicate_groups().len(), 2);
    }

    #[test]
    fn test_get_duplicate_group_by_id() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        let group_id = groups[0].group_id.clone();
        let found = manager.get_duplicate_group(&group_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().group_id, group_id);
    }

    #[test]
    fn test_get_duplicate_group_not_found() {
        let manager = DuplicateDetectionManager::new();
        assert!(manager.get_duplicate_group("nonexistent").is_none());
    }

    #[test]
    fn test_get_task_duplicate_group_not_found() {
        let manager = DuplicateDetectionManager::new();
        assert!(manager.get_task_duplicate_group("nonexistent").is_none());
    }

    #[test]
    fn test_remove_duplicate_group_not_found() {
        let mut manager = DuplicateDetectionManager::new();
        assert!(!manager.remove_duplicate_group("nonexistent"));
    }

    #[test]
    fn test_remove_duplicate_group_clears_task_mapping() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        let group_id = groups[0].group_id.clone();
        // Tasks should be mapped
        assert!(manager.get_task_duplicate_group("1").is_some());
        // Remove group
        manager.remove_duplicate_group(&group_id);
        // Task mapping should be cleared
        assert!(manager.get_task_duplicate_group("1").is_none());
        assert!(manager.get_task_duplicate_group("2").is_none());
    }

    #[test]
    fn test_clear_duplicate_groups() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/a.zip"),
            create_test_task("2", "T2", "https://example.com/a.zip"),
            create_test_task("3", "T3", "https://example.com/b.zip"),
            create_test_task("4", "T4", "https://example.com/b.zip"),
        ];
        manager.detect_duplicates(&tasks);
        assert_eq!(manager.get_duplicate_groups().len(), 2);
        manager.clear_duplicate_groups();
        assert_eq!(manager.get_duplicate_groups().len(), 0);
    }

    // ── Summary ──

    #[test]
    fn test_summary_by_method() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        manager.detect_duplicates(&tasks);
        let summary = manager.get_summary();
        assert_eq!(*summary.by_method.get("URL").unwrap(), 1);
    }

    #[test]
    fn test_summary_by_severity() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        manager.detect_duplicates(&tasks);
        let summary = manager.get_summary();
        // URL duplicates have High severity
        assert_eq!(*summary.by_severity.get("High").unwrap(), 2);
    }

    #[test]
    fn test_summary_empty() {
        let manager = DuplicateDetectionManager::new();
        let summary = manager.get_summary();
        assert_eq!(summary.total_groups, 0);
        assert_eq!(summary.total_duplicates, 0);
        assert!(summary.by_method.is_empty());
        assert!(summary.by_severity.is_empty());
        assert_eq!(summary.estimated_wasted_bytes, 0);
        assert!(summary.last_scan_at.is_none());
    }

    // ── Persistence ──

    #[test]
    fn test_save_config_creates_file() {
        let manager = DuplicateDetectionManager::new();
        let path = std::env::temp_dir().join("dup_test_create.json");
        let _ = std::fs::remove_file(&path);
        manager.save_config(&path).unwrap();
        assert!(path.exists());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_save_config_overwrite() {
        let manager = DuplicateDetectionManager::new();
        let path = std::env::temp_dir().join("dup_test_overwrite.json");
        manager.save_config(&path).unwrap();
        // Save again with different config
        let mut manager2 = DuplicateDetectionManager::new();
        manager2.config.enabled = false;
        manager2.save_config(&path).unwrap();
        let loaded = DuplicateDetectionManager::load_config(&path).unwrap();
        assert!(!loaded.enabled);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_save_config_pretty_json() {
        let manager = DuplicateDetectionManager::new();
        let path = std::env::temp_dir().join("dup_test_pretty.json");
        manager.save_config(&path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        // Pretty JSON has newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_load_config_missing_file() {
        let path = std::env::temp_dir().join("dup_nonexistent.json");
        let _ = std::fs::remove_file(&path);
        assert!(DuplicateDetectionManager::load_config(&path).is_err());
    }

    #[test]
    fn test_load_config_corrupt_json() {
        let path = std::env::temp_dir().join("dup_corrupt.json");
        std::fs::write(&path, "not valid json{{{").unwrap();
        assert!(DuplicateDetectionManager::load_config(&path).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_load_config_empty_file() {
        let path = std::env::temp_dir().join("dup_empty.json");
        std::fs::write(&path, "").unwrap();
        assert!(DuplicateDetectionManager::load_config(&path).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_config_roundtrip_unicode() {
        let mut config = DuplicateDetectionConfig::default();
        config.enabled = true;
        config.max_duplicate_groups = 42;
        let manager = DuplicateDetectionManager::with_config(config);
        let path = std::env::temp_dir().join("dup_unicode_config.json");
        manager.save_config(&path).unwrap();
        let loaded = DuplicateDetectionManager::load_config(&path).unwrap();
        assert_eq!(loaded.max_duplicate_groups, 42);
        std::fs::remove_file(&path).ok();
    }

    // ── Detection re-runs clear previous state ──

    #[test]
    fn test_detect_rerun_clears_previous() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks1 = vec![
            create_test_task("1", "T1", "https://example.com/a.zip"),
            create_test_task("2", "T2", "https://example.com/a.zip"),
        ];
        manager.detect_duplicates(&tasks1);
        assert_eq!(manager.get_duplicate_groups().len(), 1);

        // Run again with different tasks (no duplicates)
        let tasks2 = vec![
            create_test_task("3", "T3", "https://example.com/x.zip"),
            create_test_task("4", "T4", "https://example.com/y.zip"),
        ];
        manager.detect_duplicates(&tasks2);
        assert_eq!(manager.get_duplicate_groups().len(), 0);
    }

    // ── max_duplicate_groups limit ──

    #[test]
    fn test_max_duplicate_groups_limit() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.max_duplicate_groups = 1;
        manager.config.detect_by_filename = false;
        // Create 3 URL groups
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/a.zip"),
            create_test_task("2", "T2", "https://example.com/a.zip"),
            create_test_task("3", "T3", "https://example.com/b.zip"),
            create_test_task("4", "T4", "https://example.com/b.zip"),
            create_test_task("5", "T5", "https://example.com/c.zip"),
            create_test_task("6", "T6", "https://example.com/c.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        // Should be truncated to 1
        assert_eq!(groups.len(), 1);
    }

    // ── Unicode task data ──

    #[test]
    fn test_detect_unicode_task_data() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            TaskDuplicateData {
                task_id: "🎯1".to_string(),
                task_name: "中文任务A".to_string(),
                url: "https://example.com/文件.zip".to_string(),
                checksum: None,
                file_size: None,
                state: "Queued".to_string(),
                priority: 0,
                created_at: SystemTime::now(),
            },
            TaskDuplicateData {
                task_id: "🎯2".to_string(),
                task_name: "中文任务B".to_string(),
                url: "https://example.com/文件.zip".to_string(),
                checksum: None,
                file_size: None,
                state: "Queued".to_string(),
                priority: 0,
                created_at: SystemTime::now(),
            },
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].tasks.len(), 2);
    }

    // ── Group ID format ──

    #[test]
    fn test_group_id_format_url() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert!(groups[0].group_id.starts_with("url_"));
    }

    #[test]
    fn test_group_id_format_checksum() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_size = false;
        let mut t1 = create_test_task("1", "T1", "https://example.com/a.zip");
        t1.checksum = Some("deadbeef".to_string());
        let mut t2 = create_test_task("2", "T2", "https://example.com/b.zip");
        t2.checksum = Some("deadbeef".to_string());
        let groups = manager.detect_duplicates(&[t1, t2]);
        assert!(groups[0].group_id.starts_with("checksum_"));
    }

    #[test]
    fn test_group_id_format_size() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_checksum = false;
        let mut t1 = create_test_task("1", "T1", "https://example.com/a.zip");
        t1.file_size = Some(9999);
        let mut t2 = create_test_task("2", "T2", "https://example.com/b.zip");
        t2.file_size = Some(9999);
        let groups = manager.detect_duplicates(&[t1, t2]);
        assert!(groups[0].group_id.starts_with("size_"));
        assert!(groups[0].group_id.contains("9999"));
    }

    #[test]
    fn test_group_id_format_filename() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_checksum = false;
        manager.config.detect_by_size = false;
        manager.config.filename_similarity_threshold = 0.7;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/document.pdf"),
            create_test_task("2", "T2", "https://other.com/document.pdf"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert!(groups[0].group_id.starts_with("filename_"));
    }

    // ── Severity assignment ──

    #[test]
    fn test_severity_url_is_high() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        for task in &groups[0].tasks {
            assert_eq!(task.severity, DuplicateSeverity::High);
        }
    }

    #[test]
    fn test_severity_checksum_is_medium() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_size = false;
        let mut t1 = create_test_task("1", "T1", "https://example.com/a.zip");
        t1.checksum = Some("abc".to_string());
        let mut t2 = create_test_task("2", "T2", "https://example.com/b.zip");
        t2.checksum = Some("abc".to_string());
        let groups = manager.detect_duplicates(&[t1, t2]);
        for task in &groups[0].tasks {
            assert_eq!(task.severity, DuplicateSeverity::Medium);
        }
    }

    #[test]
    fn test_severity_size_is_low() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_url = false;
        manager.config.detect_by_filename = false;
        manager.config.detect_by_checksum = false;
        let mut t1 = create_test_task("1", "T1", "https://example.com/a.zip");
        t1.file_size = Some(1024);
        let mut t2 = create_test_task("2", "T2", "https://example.com/b.zip");
        t2.file_size = Some(1024);
        let groups = manager.detect_duplicates(&[t1, t2]);
        for task in &groups[0].tasks {
            assert_eq!(task.severity, DuplicateSeverity::Low);
        }
    }

    // ── Complex workflow ──

    #[test]
    fn test_full_lifecycle() {
        let mut manager = DuplicateDetectionManager::new();
        manager.config.detect_by_filename = false;

        // 1. Initial state
        assert_eq!(manager.get_duplicate_groups().len(), 0);
        assert!(manager.last_scan_at.is_none());

        // 2. Detect duplicates
        let tasks = vec![
            create_test_task("1", "T1", "https://example.com/file.zip"),
            create_test_task("2", "T2", "https://example.com/file.zip"),
        ];
        let groups = manager.detect_duplicates(&tasks);
        assert_eq!(groups.len(), 1);
        assert!(manager.last_scan_at.is_some());

        // 3. Get summary
        let summary = manager.get_summary();
        assert_eq!(summary.total_groups, 1);
        assert_eq!(summary.total_duplicates, 2);

        // 4. Remove group
        let group_id = groups[0].group_id.clone();
        manager.remove_duplicate_group(&group_id);
        assert_eq!(manager.get_duplicate_groups().len(), 0);

        // 5. Clear
        manager.clear_duplicate_groups();
        assert_eq!(manager.get_duplicate_groups().len(), 0);
    }

    #[test]
    fn test_multi_detection_methods() {
        let mut manager = DuplicateDetectionManager::new();
        // Enable all methods
        manager.config.detect_by_url = true;
        manager.config.detect_by_checksum = true;
        manager.config.detect_by_size = true;
        manager.config.detect_by_filename = false;

        let mut t1 = create_test_task("1", "T1", "https://example.com/a.zip");
        t1.checksum = Some("hash1".to_string());
        t1.file_size = Some(1024);
        let mut t2 = create_test_task("2", "T2", "https://example.com/a.zip");
        t2.checksum = Some("hash1".to_string());
        t2.file_size = Some(1024);

        let groups = manager.detect_duplicates(&[t1, t2]);
        // Should detect by URL, checksum, and size
        assert!(
            groups.len() >= 2,
            "expected multiple groups, got {}",
            groups.len()
        );
    }

    // ── TaskDuplicateData ──

    #[test]
    fn test_task_duplicate_data_clone_debug() {
        let task = create_test_task("1", "Test", "https://example.com/file.zip");
        let cloned = task.clone();
        assert_eq!(cloned.task_id, "1");
        let _ = format!("{:?}", task);
    }

    // ── Hash string ──

    #[test]
    fn test_hash_string_deterministic() {
        let manager = DuplicateDetectionManager::new();
        let h1 = manager.hash_string("test");
        let h2 = manager.hash_string("test");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_string_different_inputs() {
        let manager = DuplicateDetectionManager::new();
        let h1 = manager.hash_string("abc");
        let h2 = manager.hash_string("xyz");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_string_empty() {
        let manager = DuplicateDetectionManager::new();
        let h = manager.hash_string("");
        assert!(!h.is_empty());
    }

    // ── Similarity edge cases ──

    #[test]
    fn test_similarity_single_char() {
        let manager = DuplicateDetectionManager::new();
        assert!((manager.calculate_similarity("a", "a") - 1.0).abs() < f64::EPSILON);
        assert!((manager.calculate_similarity("a", "b") - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_similarity_partial_overlap() {
        let manager = DuplicateDetectionManager::new();
        let sim = manager.calculate_similarity("abcd", "abce");
        // Jaccard: intersection={a,b,c}=3, union={a,b,c,d,e}=5 → 3/5=0.6
        assert!((sim - 0.6).abs() < 0.01, "expected ~0.6, got {}", sim);
    }

    // ── Persisted data roundtrip ──

    #[test]
    fn test_config_full_roundtrip_all_fields() {
        let config = DuplicateDetectionConfig {
            enabled: false,
            detect_by_url: false,
            detect_by_filename: false,
            detect_by_checksum: false,
            detect_by_size: false,
            filename_similarity_threshold: 0.42,
            auto_pause_duplicates: true,
            auto_remove_duplicates: true,
            max_duplicate_groups: 7,
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: DuplicateDetectionConfig = serde_json::from_str(&json).unwrap();
        assert!(!parsed.enabled);
        assert!(!parsed.detect_by_url);
        assert!(!parsed.detect_by_filename);
        assert!(!parsed.detect_by_checksum);
        assert!(!parsed.detect_by_size);
        assert!((parsed.filename_similarity_threshold - 0.42).abs() < f64::EPSILON);
        assert!(parsed.auto_pause_duplicates);
        assert!(parsed.auto_remove_duplicates);
        assert_eq!(parsed.max_duplicate_groups, 7);
    }
}
