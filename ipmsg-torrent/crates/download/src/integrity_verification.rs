//! Download integrity verification system
//!
//! Verifies completed download files still exist on disk with correct sizes,
//! detects external file changes, and provides repair capabilities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

/// Verification status for a download task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    /// File exists and size matches expected
    Verified,
    /// File exists but size differs from expected
    SizeMismatch,
    /// File does not exist on disk
    Missing,
    /// File exists but is empty (0 bytes)
    Empty,
    /// Verification not yet performed
    Pending,
    /// Verification failed due to I/O error
    Error,
}

impl std::fmt::Display for VerificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verified => write!(f, "✅ Verified"),
            Self::SizeMismatch => write!(f, "⚠️ Size mismatch"),
            Self::Missing => write!(f, "❌ Missing"),
            Self::Empty => write!(f, "⚠️ Empty file"),
            Self::Pending => write!(f, "⏳ Pending"),
            Self::Error => write!(f, "❌ Error"),
        }
    }
}

/// Verification result for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Expected file size in bytes
    pub expected_size: u64,
    /// Actual file size on disk (0 if missing)
    pub actual_size: u64,
    /// Verification status
    pub status: VerificationStatus,
    /// Path to the file
    pub file_path: PathBuf,
    /// Timestamp of last verification
    pub verified_at: DateTime<Utc>,
    /// Error message if status is Error
    pub error_message: Option<String>,
}

impl VerificationResult {
    /// Check if the file needs repair
    pub fn needs_repair(&self) -> bool {
        matches!(
            self.status,
            VerificationStatus::Missing
                | VerificationStatus::SizeMismatch
                | VerificationStatus::Empty
        )
    }
}

/// Configuration for integrity verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityConfig {
    /// Enable automatic verification after download completion
    pub auto_verify_on_complete: bool,
    /// Enable periodic background verification
    pub periodic_verification: bool,
    /// Interval for periodic verification in seconds (default: 3600 = 1 hour)
    pub verification_interval_secs: u64,
    /// Only verify tasks in Complete state
    pub only_verify_completed: bool,
    /// Maximum number of tasks to verify in one batch
    pub max_batch_size: usize,
}

impl Default for IntegrityConfig {
    fn default() -> Self {
        Self {
            auto_verify_on_complete: true,
            periodic_verification: false,
            verification_interval_secs: 3600,
            only_verify_completed: true,
            max_batch_size: 100,
        }
    }
}

/// Summary of integrity verification results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegritySummary {
    /// Total tasks verified
    pub total_tasks: usize,
    /// Tasks with verified files
    pub verified: usize,
    /// Tasks with size mismatch
    pub size_mismatch: usize,
    /// Tasks with missing files
    pub missing: usize,
    /// Tasks with empty files
    pub empty: usize,
    /// Tasks with verification errors
    pub errors: usize,
    /// Tasks pending verification
    pub pending: usize,
    /// Timestamp of this summary
    pub generated_at: DateTime<Utc>,
}

impl IntegritySummary {
    /// Create a new empty summary
    pub fn new() -> Self {
        Self {
            total_tasks: 0,
            verified: 0,
            size_mismatch: 0,
            missing: 0,
            empty: 0,
            errors: 0,
            pending: 0,
            generated_at: Utc::now(),
        }
    }

    /// Add a verification result to the summary
    pub fn add_result(&mut self, result: &VerificationResult) {
        self.total_tasks += 1;
        match result.status {
            VerificationStatus::Verified => self.verified += 1,
            VerificationStatus::SizeMismatch => self.size_mismatch += 1,
            VerificationStatus::Missing => self.missing += 1,
            VerificationStatus::Empty => self.empty += 1,
            VerificationStatus::Pending => self.pending += 1,
            VerificationStatus::Error => self.errors += 1,
        }
    }

    /// Check if all tasks are verified
    pub fn all_verified(&self) -> bool {
        self.verified == self.total_tasks && self.total_tasks > 0
    }

    /// Check if there are any issues
    pub fn has_issues(&self) -> bool {
        self.size_mismatch > 0 || self.missing > 0 || self.empty > 0 || self.errors > 0
    }
}

impl Default for IntegritySummary {
    fn default() -> Self {
        Self::new()
    }
}

/// Integrity verification manager
#[derive(Debug)]
pub struct IntegrityManager {
    /// Configuration
    config: IntegrityConfig,
    /// Verification results (task_id -> result)
    results: HashMap<String, VerificationResult>,
    /// Timestamp of last full verification
    last_full_verification: Option<DateTime<Utc>>,
}

impl IntegrityManager {
    /// Create a new integrity manager
    pub fn new() -> Self {
        Self {
            config: IntegrityConfig::default(),
            results: HashMap::new(),
            last_full_verification: None,
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: IntegrityConfig) -> Self {
        Self {
            config,
            results: HashMap::new(),
            last_full_verification: None,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &IntegrityConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: IntegrityConfig) {
        self.config = config;
    }

    /// Verify a single file
    pub async fn verify_file(
        &mut self,
        task_id: String,
        task_name: String,
        file_path: PathBuf,
        expected_size: u64,
    ) -> VerificationResult {
        let result = match fs::metadata(&file_path).await {
            Ok(metadata) => {
                let actual_size = metadata.len();
                let status = if actual_size == 0 {
                    VerificationStatus::Empty
                } else if actual_size == expected_size {
                    VerificationStatus::Verified
                } else {
                    VerificationStatus::SizeMismatch
                };

                VerificationResult {
                    task_id: task_id.clone(),
                    task_name: task_name.clone(),
                    expected_size,
                    actual_size,
                    status,
                    file_path: file_path.clone(),
                    verified_at: Utc::now(),
                    error_message: None,
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => VerificationResult {
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                expected_size,
                actual_size: 0,
                status: VerificationStatus::Missing,
                file_path: file_path.clone(),
                verified_at: Utc::now(),
                error_message: None,
            },
            Err(e) => VerificationResult {
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                expected_size,
                actual_size: 0,
                status: VerificationStatus::Error,
                file_path: file_path.clone(),
                verified_at: Utc::now(),
                error_message: Some(e.to_string()),
            },
        };

        self.results.insert(task_id, result.clone());
        result
    }

    /// Verify multiple files in batch
    pub async fn verify_batch(
        &mut self,
        tasks: Vec<(String, String, PathBuf, u64)>,
    ) -> Vec<VerificationResult> {
        let mut results = Vec::with_capacity(tasks.len());
        let batch_size = tasks.len().min(self.config.max_batch_size);

        for (task_id, task_name, file_path, expected_size) in tasks.into_iter().take(batch_size) {
            let result = self
                .verify_file(task_id, task_name, file_path, expected_size)
                .await;
            results.push(result);
        }

        self.last_full_verification = Some(Utc::now());
        results
    }

    /// Get verification result for a task
    pub fn get_result(&self, task_id: &str) -> Option<&VerificationResult> {
        self.results.get(task_id)
    }

    /// Get all verification results
    pub fn all_results(&self) -> Vec<&VerificationResult> {
        self.results.values().collect()
    }

    /// Get results filtered by status
    pub fn results_by_status(&self, status: VerificationStatus) -> Vec<&VerificationResult> {
        self.results
            .values()
            .filter(|r| r.status == status)
            .collect()
    }

    /// Get tasks that need repair
    pub fn tasks_needing_repair(&self) -> Vec<&VerificationResult> {
        self.results.values().filter(|r| r.needs_repair()).collect()
    }

    /// Remove verification result for a task
    pub fn remove_result(&mut self, task_id: &str) -> bool {
        self.results.remove(task_id).is_some()
    }

    /// Clear all verification results
    pub fn clear(&mut self) {
        self.results.clear();
        self.last_full_verification = None;
    }

    /// Generate summary of verification results
    pub fn summary(&self) -> IntegritySummary {
        let mut summary = IntegritySummary::new();
        for result in self.results.values() {
            summary.add_result(result);
        }
        summary
    }

    /// Get timestamp of last full verification
    pub fn last_full_verification(&self) -> Option<DateTime<Utc>> {
        self.last_full_verification
    }

    /// Save configuration to disk
    pub async fn save_config(&self, data_dir: &std::path::Path) -> Result<(), std::io::Error> {
        let config_path = data_dir.join("integrity_config.json");
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;

        // Atomic write
        let temp_path = data_dir.join("integrity_config.json.tmp");
        fs::write(&temp_path, json.as_bytes()).await?;
        fs::rename(&temp_path, &config_path).await?;

        Ok(())
    }

    /// Load configuration from disk
    pub async fn load_config(
        data_dir: &std::path::Path,
    ) -> Result<IntegrityConfig, std::io::Error> {
        let config_path = data_dir.join("integrity_config.json");

        match fs::read_to_string(&config_path).await {
            Ok(content) => serde_json::from_str(&content)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(IntegrityConfig::default()),
            Err(e) => Err(e),
        }
    }
}

impl Default for IntegrityManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_verification_status_display() {
        assert_eq!(VerificationStatus::Verified.to_string(), "✅ Verified");
        assert_eq!(VerificationStatus::Missing.to_string(), "❌ Missing");
        assert_eq!(
            VerificationStatus::SizeMismatch.to_string(),
            "⚠️ Size mismatch"
        );
        assert_eq!(VerificationStatus::Empty.to_string(), "⚠️ Empty file");
        assert_eq!(VerificationStatus::Pending.to_string(), "⏳ Pending");
        assert_eq!(VerificationStatus::Error.to_string(), "❌ Error");
    }

    #[test]
    fn test_verification_result_needs_repair() {
        let result = VerificationResult {
            task_id: "test".to_string(),
            task_name: "Test".to_string(),
            expected_size: 1000,
            actual_size: 1000,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/test"),
            verified_at: Utc::now(),
            error_message: None,
        };
        assert!(!result.needs_repair());

        let result_missing = VerificationResult {
            status: VerificationStatus::Missing,
            ..result.clone()
        };
        assert!(result_missing.needs_repair());

        let result_mismatch = VerificationResult {
            status: VerificationStatus::SizeMismatch,
            ..result.clone()
        };
        assert!(result_mismatch.needs_repair());

        let result_empty = VerificationResult {
            status: VerificationStatus::Empty,
            ..result.clone()
        };
        assert!(result_empty.needs_repair());
    }

    #[test]
    fn test_integrity_config_default() {
        let config = IntegrityConfig::default();
        assert!(config.auto_verify_on_complete);
        assert!(!config.periodic_verification);
        assert_eq!(config.verification_interval_secs, 3600);
        assert!(config.only_verify_completed);
        assert_eq!(config.max_batch_size, 100);
    }

    #[test]
    fn test_integrity_summary_new() {
        let summary = IntegritySummary::new();
        assert_eq!(summary.total_tasks, 0);
        assert_eq!(summary.verified, 0);
        assert!(!summary.all_verified());
        assert!(!summary.has_issues());
    }

    #[test]
    fn test_integrity_summary_add_result() {
        let mut summary = IntegritySummary::new();

        let verified = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task 1".to_string(),
            expected_size: 1000,
            actual_size: 1000,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&verified);

        let missing = VerificationResult {
            task_id: "2".to_string(),
            task_name: "Task 2".to_string(),
            expected_size: 2000,
            actual_size: 0,
            status: VerificationStatus::Missing,
            file_path: PathBuf::from("/tmp/2"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&missing);

        assert_eq!(summary.total_tasks, 2);
        assert_eq!(summary.verified, 1);
        assert_eq!(summary.missing, 1);
        assert!(!summary.all_verified());
        assert!(summary.has_issues());
    }

    #[test]
    fn test_integrity_summary_all_verified() {
        let mut summary = IntegritySummary::new();

        let result1 = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task 1".to_string(),
            expected_size: 1000,
            actual_size: 1000,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&result1);

        let result2 = VerificationResult {
            task_id: "2".to_string(),
            task_name: "Task 2".to_string(),
            expected_size: 2000,
            actual_size: 2000,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/2"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&result2);

        assert!(summary.all_verified());
        assert!(!summary.has_issues());
    }

    #[test]
    fn test_integrity_manager_new() {
        let manager = IntegrityManager::new();
        assert!(manager.config().auto_verify_on_complete);
        assert!(manager.all_results().is_empty());
        assert!(manager.last_full_verification().is_none());
    }

    #[test]
    fn test_integrity_manager_with_config() {
        let config = IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 1800,
            only_verify_completed: false,
            max_batch_size: 50,
        };
        let manager = IntegrityManager::with_config(config);
        assert!(!manager.config().auto_verify_on_complete);
        assert!(manager.config().periodic_verification);
        assert_eq!(manager.config().verification_interval_secs, 1800);
    }

    #[test]
    fn test_integrity_manager_set_config() {
        let mut manager = IntegrityManager::new();
        let new_config = IntegrityConfig {
            auto_verify_on_complete: false,
            ..IntegrityConfig::default()
        };
        manager.set_config(new_config);
        assert!(!manager.config().auto_verify_on_complete);
    }

    #[tokio::test]
    async fn test_verify_file_verified() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello world").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "task1".to_string(),
                "Test Task".to_string(),
                file_path.clone(),
                11,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::Verified);
        assert_eq!(result.expected_size, 11);
        assert_eq!(result.actual_size, 11);
        assert!(!result.needs_repair());
    }

    #[tokio::test]
    async fn test_verify_file_size_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "task1".to_string(),
                "Test Task".to_string(),
                file_path.clone(),
                100,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::SizeMismatch);
        assert_eq!(result.expected_size, 100);
        assert_eq!(result.actual_size, 5);
        assert!(result.needs_repair());
    }

    #[tokio::test]
    async fn test_verify_file_missing() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "task1".to_string(),
                "Test Task".to_string(),
                file_path.clone(),
                100,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::Missing);
        assert_eq!(result.actual_size, 0);
        assert!(result.needs_repair());
    }

    #[tokio::test]
    async fn test_verify_file_empty() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.txt");
        tokio::fs::write(&file_path, b"").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "task1".to_string(),
                "Test Task".to_string(),
                file_path.clone(),
                100,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::Empty);
        assert_eq!(result.actual_size, 0);
        assert!(result.needs_repair());
    }

    #[tokio::test]
    async fn test_verify_batch() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        tokio::fs::write(&file1, b"content1").await.unwrap();

        let file2 = temp_dir.path().join("file2.txt");
        tokio::fs::write(&file2, b"content2content2").await.unwrap();

        let tasks = vec![
            ("task1".to_string(), "Task 1".to_string(), file1.clone(), 8),
            ("task2".to_string(), "Task 2".to_string(), file2.clone(), 16),
        ];

        let mut manager = IntegrityManager::new();
        let results = manager.verify_batch(tasks).await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, VerificationStatus::Verified);
        assert_eq!(results[1].status, VerificationStatus::Verified);
        assert!(manager.last_full_verification().is_some());
    }

    #[tokio::test]
    async fn test_verify_batch_respects_max_size() {
        let temp_dir = TempDir::new().unwrap();

        let mut tasks = Vec::new();
        for i in 0..10 {
            let file = temp_dir.path().join(format!("file{}.txt", i));
            tokio::fs::write(&file, b"content").await.unwrap();
            tasks.push((format!("task{}", i), format!("Task {}", i), file, 7));
        }

        let mut manager = IntegrityManager::with_config(IntegrityConfig {
            max_batch_size: 5,
            ..IntegrityConfig::default()
        });
        let results = manager.verify_batch(tasks).await;

        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_get_result() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Test".to_string(), file_path, 5)
            .await;

        let result = manager.get_result("task1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().status, VerificationStatus::Verified);

        let result = manager.get_result("nonexistent");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_all_results() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        tokio::fs::write(&file1, b"a").await.unwrap();

        let file2 = temp_dir.path().join("file2.txt");
        tokio::fs::write(&file2, b"bb").await.unwrap();

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Task 1".to_string(), file1, 1)
            .await;
        manager
            .verify_file("task2".to_string(), "Task 2".to_string(), file2, 2)
            .await;

        let results = manager.all_results();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_results_by_status() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        tokio::fs::write(&file1, b"content").await.unwrap();

        let file2 = temp_dir.path().join("file2.txt");
        // file2 doesn't exist, will be Missing

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Task 1".to_string(), file1, 7)
            .await;
        manager
            .verify_file("task2".to_string(), "Task 2".to_string(), file2, 100)
            .await;

        let verified = manager.results_by_status(VerificationStatus::Verified);
        assert_eq!(verified.len(), 1);

        let missing = manager.results_by_status(VerificationStatus::Missing);
        assert_eq!(missing.len(), 1);
    }

    #[tokio::test]
    async fn test_tasks_needing_repair() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        tokio::fs::write(&file1, b"content").await.unwrap();

        let file2 = temp_dir.path().join("file2.txt");
        // file2 doesn't exist

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Task 1".to_string(), file1, 7)
            .await;
        manager
            .verify_file("task2".to_string(), "Task 2".to_string(), file2, 100)
            .await;

        let repair_needed = manager.tasks_needing_repair();
        assert_eq!(repair_needed.len(), 1);
        assert_eq!(repair_needed[0].task_id, "task2");
    }

    #[tokio::test]
    async fn test_remove_result() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Test".to_string(), file_path, 5)
            .await;

        assert!(manager.get_result("task1").is_some());
        assert!(manager.remove_result("task1"));
        assert!(manager.get_result("task1").is_none());
        assert!(!manager.remove_result("nonexistent"));
    }

    #[tokio::test]
    async fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Test".to_string(), file_path, 5)
            .await;

        assert!(!manager.all_results().is_empty());
        manager.clear();
        assert!(manager.all_results().is_empty());
        assert!(manager.last_full_verification().is_none());
    }

    #[tokio::test]
    async fn test_summary() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        tokio::fs::write(&file1, b"content").await.unwrap();

        let file2 = temp_dir.path().join("file2.txt");
        // file2 doesn't exist

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Task 1".to_string(), file1, 7)
            .await;
        manager
            .verify_file("task2".to_string(), "Task 2".to_string(), file2, 100)
            .await;

        let summary = manager.summary();
        assert_eq!(summary.total_tasks, 2);
        assert_eq!(summary.verified, 1);
        assert_eq!(summary.missing, 1);
        assert!(summary.has_issues());
    }

    #[tokio::test]
    async fn test_save_and_load_config() {
        let temp_dir = TempDir::new().unwrap();

        let mut manager = IntegrityManager::new();
        manager.set_config(IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 7200,
            only_verify_completed: false,
            max_batch_size: 200,
        });

        manager.save_config(temp_dir.path()).await.unwrap();

        let loaded = IntegrityManager::load_config(temp_dir.path())
            .await
            .unwrap();
        assert!(!loaded.auto_verify_on_complete);
        assert!(loaded.periodic_verification);
        assert_eq!(loaded.verification_interval_secs, 7200);
        assert!(!loaded.only_verify_completed);
        assert_eq!(loaded.max_batch_size, 200);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let temp_dir = TempDir::new().unwrap();

        let loaded = IntegrityManager::load_config(temp_dir.path())
            .await
            .unwrap();
        assert_eq!(loaded.verification_interval_secs, 3600);
    }
}
