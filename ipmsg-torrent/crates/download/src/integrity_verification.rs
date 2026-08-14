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

    // ========== Phase 215: Comprehensive Test Coverage ==========

    // --- VerificationStatus serde tests ---
    #[test]
    fn test_verification_status_serde_roundtrip() {
        let statuses = [
            VerificationStatus::Verified,
            VerificationStatus::SizeMismatch,
            VerificationStatus::Missing,
            VerificationStatus::Empty,
            VerificationStatus::Pending,
            VerificationStatus::Error,
        ];

        for status in &statuses {
            let json = serde_json::to_string(status).unwrap();
            let deserialized: VerificationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*status, deserialized);
        }
    }

    #[test]
    fn test_verification_status_snake_case_serde() {
        assert_eq!(
            serde_json::to_string(&VerificationStatus::SizeMismatch).unwrap(),
            "\"size_mismatch\""
        );
        assert_eq!(
            serde_json::from_str::<VerificationStatus>("\"size_mismatch\"").unwrap(),
            VerificationStatus::SizeMismatch
        );
    }

    #[test]
    fn test_verification_status_clone_copy_debug() {
        // Clone
        let status = VerificationStatus::Verified;
        let cloned = status.clone();
        assert_eq!(status, cloned);

        // Copy (implicit)
        let copied = status;
        assert_eq!(copied, VerificationStatus::Verified);

        // Debug
        let debug_str = format!("{:?}", VerificationStatus::SizeMismatch);
        assert!(debug_str.contains("SizeMismatch"));
    }

    #[test]
    fn test_verification_status_partial_eq() {
        assert_eq!(VerificationStatus::Verified, VerificationStatus::Verified);
        assert_ne!(VerificationStatus::Verified, VerificationStatus::Missing);
        assert_ne!(VerificationStatus::Empty, VerificationStatus::Pending);
    }

    // --- VerificationResult serde tests ---
    #[test]
    fn test_verification_result_serde_roundtrip() {
        let result = VerificationResult {
            task_id: "task-123".to_string(),
            task_name: "Test Download".to_string(),
            expected_size: 1_000_000,
            actual_size: 1_000_000,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/downloads/test.zip"),
            verified_at: Utc::now(),
            error_message: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: VerificationResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task_id, "task-123");
        assert_eq!(deserialized.task_name, "Test Download");
        assert_eq!(deserialized.expected_size, 1_000_000);
        assert_eq!(deserialized.actual_size, 1_000_000);
        assert_eq!(deserialized.status, VerificationStatus::Verified);
        assert_eq!(deserialized.file_path, PathBuf::from("/downloads/test.zip"));
        assert!(deserialized.error_message.is_none());
    }

    #[test]
    fn test_verification_result_serde_with_error() {
        let result = VerificationResult {
            task_id: "task-err".to_string(),
            task_name: "Error Task".to_string(),
            expected_size: 500,
            actual_size: 0,
            status: VerificationStatus::Error,
            file_path: PathBuf::from("/tmp/error"),
            verified_at: Utc::now(),
            error_message: Some("Permission denied".to_string()),
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: VerificationResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.status, VerificationStatus::Error);
        assert_eq!(
            deserialized.error_message,
            Some("Permission denied".to_string())
        );
    }

    #[test]
    fn test_verification_result_clone_debug() {
        let result = VerificationResult {
            task_id: "t1".to_string(),
            task_name: "Task".to_string(),
            expected_size: 100,
            actual_size: 100,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/test"),
            verified_at: Utc::now(),
            error_message: None,
        };

        let cloned = result.clone();
        assert_eq!(cloned.task_id, "t1");

        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("task_id"));
    }

    #[test]
    fn test_verification_result_needs_repair_all_statuses() {
        let base = VerificationResult {
            task_id: "test".to_string(),
            task_name: "Test".to_string(),
            expected_size: 1000,
            actual_size: 1000,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/test"),
            verified_at: Utc::now(),
            error_message: None,
        };

        // Verified - no repair
        assert!(!base.needs_repair());

        // Pending - no repair
        let pending = VerificationResult {
            status: VerificationStatus::Pending,
            ..base.clone()
        };
        assert!(!pending.needs_repair());

        // Error - no repair
        let error = VerificationResult {
            status: VerificationStatus::Error,
            ..base.clone()
        };
        assert!(!error.needs_repair());

        // Missing - needs repair
        let missing = VerificationResult {
            status: VerificationStatus::Missing,
            ..base.clone()
        };
        assert!(missing.needs_repair());

        // SizeMismatch - needs repair
        let mismatch = VerificationResult {
            status: VerificationStatus::SizeMismatch,
            ..base.clone()
        };
        assert!(mismatch.needs_repair());

        // Empty - needs repair
        let empty = VerificationResult {
            status: VerificationStatus::Empty,
            ..base.clone()
        };
        assert!(empty.needs_repair());
    }

    // --- IntegrityConfig serde tests ---
    #[test]
    fn test_integrity_config_serde_roundtrip() {
        let config = IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 7200,
            only_verify_completed: false,
            max_batch_size: 50,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: IntegrityConfig = serde_json::from_str(&json).unwrap();

        assert!(!deserialized.auto_verify_on_complete);
        assert!(deserialized.periodic_verification);
        assert_eq!(deserialized.verification_interval_secs, 7200);
        assert!(!deserialized.only_verify_completed);
        assert_eq!(deserialized.max_batch_size, 50);
    }

    #[test]
    fn test_integrity_config_pretty_serde() {
        let config = IntegrityConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();

        assert!(pretty.contains('\n'));
        assert!(pretty.contains("auto_verify_on_complete"));
        assert!(pretty.contains("verification_interval_secs"));
    }

    #[test]
    fn test_integrity_config_extra_fields_ignored() {
        let json = r#"{
            "auto_verify_on_complete": true,
            "periodic_verification": false,
            "verification_interval_secs": 3600,
            "only_verify_completed": true,
            "max_batch_size": 100,
            "unknown_field": "should be ignored",
            "another_extra": 42
        }"#;

        let config: IntegrityConfig = serde_json::from_str(json).unwrap();
        assert!(config.auto_verify_on_complete);
        assert_eq!(config.verification_interval_secs, 3600);
    }

    #[test]
    fn test_integrity_config_custom_values() {
        let config = IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 0,
            only_verify_completed: false,
            max_batch_size: 0,
        };

        assert!(!config.auto_verify_on_complete);
        assert!(config.periodic_verification);
        assert_eq!(config.verification_interval_secs, 0);
        assert!(!config.only_verify_completed);
        assert_eq!(config.max_batch_size, 0);
    }

    #[test]
    fn test_integrity_config_large_values() {
        let config = IntegrityConfig {
            auto_verify_on_complete: true,
            periodic_verification: true,
            verification_interval_secs: u64::MAX,
            only_verify_completed: true,
            max_batch_size: usize::MAX,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: IntegrityConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.verification_interval_secs, u64::MAX);
        assert_eq!(deserialized.max_batch_size, usize::MAX);
    }

    #[test]
    fn test_integrity_config_clone_debug() {
        let config = IntegrityConfig::default();
        let cloned = config.clone();
        assert_eq!(
            cloned.auto_verify_on_complete,
            config.auto_verify_on_complete
        );

        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("IntegrityConfig"));
    }

    // --- IntegritySummary serde tests ---
    #[test]
    fn test_integrity_summary_serde_roundtrip() {
        let mut summary = IntegritySummary::new();

        let result = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task".to_string(),
            expected_size: 100,
            actual_size: 100,
            status: VerificationStatus::Verified,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&result);

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: IntegritySummary = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_tasks, 1);
        assert_eq!(deserialized.verified, 1);
    }

    #[test]
    fn test_integrity_summary_clone_debug() {
        let summary = IntegritySummary::new();
        let cloned = summary.clone();
        assert_eq!(cloned.total_tasks, 0);

        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("IntegritySummary"));
    }

    #[test]
    fn test_integrity_summary_all_counts() {
        let mut summary = IntegritySummary::new();

        let statuses = [
            ("1", VerificationStatus::Verified, 100, 100),
            ("2", VerificationStatus::SizeMismatch, 100, 50),
            ("3", VerificationStatus::Missing, 100, 0),
            ("4", VerificationStatus::Empty, 100, 0),
            ("5", VerificationStatus::Pending, 100, 0),
            ("6", VerificationStatus::Error, 100, 0),
        ];

        for (id, status, expected, actual) in statuses {
            let result = VerificationResult {
                task_id: id.to_string(),
                task_name: format!("Task {}", id),
                expected_size: expected,
                actual_size: actual,
                status,
                file_path: PathBuf::from(format!("/tmp/{}", id)),
                verified_at: Utc::now(),
                error_message: None,
            };
            summary.add_result(&result);
        }

        assert_eq!(summary.total_tasks, 6);
        assert_eq!(summary.verified, 1);
        assert_eq!(summary.size_mismatch, 1);
        assert_eq!(summary.missing, 1);
        assert_eq!(summary.empty, 1);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.errors, 1);
        assert!(summary.has_issues());
        assert!(!summary.all_verified());
    }

    #[test]
    fn test_integrity_summary_default() {
        let summary = IntegritySummary::default();
        assert_eq!(summary.total_tasks, 0);
        assert_eq!(summary.verified, 0);
        assert!(!summary.all_verified());
        assert!(!summary.has_issues());
    }

    // --- IntegrityManager default test ---
    #[test]
    fn test_integrity_manager_default() {
        let manager = IntegrityManager::default();
        assert!(manager.config().auto_verify_on_complete);
        assert!(manager.all_results().is_empty());
        assert!(manager.last_full_verification().is_none());
    }

    // --- Unicode tests ---
    #[tokio::test]
    async fn test_verify_file_unicode_task_name() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"content").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "task-unicode".to_string(),
                "中文任务名 🎉".to_string(),
                file_path,
                7,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::Verified);
        assert_eq!(result.task_name, "中文任务名 🎉");
    }

    #[tokio::test]
    async fn test_verify_file_unicode_path() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("中文文件.txt");
        tokio::fs::write(&file_path, b"data").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "task1".to_string(),
                "Task".to_string(),
                file_path.clone(),
                4,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::Verified);
        assert_eq!(result.file_path, file_path);
    }

    // --- Large file tests ---
    #[tokio::test]
    async fn test_verify_file_large_size() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("large.bin");
        // Create a file with specific size
        let content = vec![0u8; 1024 * 1024]; // 1MB
        tokio::fs::write(&file_path, &content).await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "large-task".to_string(),
                "Large File".to_string(),
                file_path,
                1024 * 1024,
            )
            .await;

        assert_eq!(result.status, VerificationStatus::Verified);
        assert_eq!(result.actual_size, 1024 * 1024);
    }

    #[tokio::test]
    async fn test_verify_file_zero_expected_size() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("zero.txt");
        tokio::fs::write(&file_path, b"").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file("task1".to_string(), "Task".to_string(), file_path, 0)
            .await;

        // Empty file with 0 expected size should be Empty status
        assert_eq!(result.status, VerificationStatus::Empty);
    }

    // --- Batch tests ---
    #[tokio::test]
    async fn test_verify_batch_empty() {
        let mut manager = IntegrityManager::new();
        let results = manager.verify_batch(vec![]).await;
        assert!(results.is_empty());
        assert!(manager.last_full_verification().is_some());
    }

    #[tokio::test]
    async fn test_verify_batch_mixed_statuses() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        tokio::fs::write(&file1, b"content").await.unwrap();

        let file2 = temp_dir.path().join("file2.txt");
        // file2 doesn't exist

        let file3 = temp_dir.path().join("file3.txt");
        tokio::fs::write(&file3, b"").await.unwrap();

        let tasks = vec![
            ("task1".to_string(), "Task 1".to_string(), file1, 7),
            ("task2".to_string(), "Task 2".to_string(), file2, 100),
            ("task3".to_string(), "Task 3".to_string(), file3, 50),
        ];

        let mut manager = IntegrityManager::new();
        let results = manager.verify_batch(tasks).await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].status, VerificationStatus::Verified);
        assert_eq!(results[1].status, VerificationStatus::Missing);
        assert_eq!(results[2].status, VerificationStatus::Empty);
    }

    #[tokio::test]
    async fn test_verify_batch_max_size_zero() {
        let temp_dir = TempDir::new().unwrap();

        let file = temp_dir.path().join("file.txt");
        tokio::fs::write(&file, b"content").await.unwrap();

        let tasks = vec![("task1".to_string(), "Task".to_string(), file, 7)];

        let mut manager = IntegrityManager::with_config(IntegrityConfig {
            max_batch_size: 0,
            ..IntegrityConfig::default()
        });
        let results = manager.verify_batch(tasks).await;

        assert!(results.is_empty());
    }

    // --- Manager result storage tests ---
    #[tokio::test]
    async fn test_verify_file_overwrites_previous_result() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::new();

        // First verification
        manager
            .verify_file(
                "task1".to_string(),
                "Task".to_string(),
                file_path.clone(),
                5,
            )
            .await;
        assert_eq!(
            manager.get_result("task1").unwrap().status,
            VerificationStatus::Verified
        );

        // Second verification with different expected size
        manager
            .verify_file("task1".to_string(), "Task".to_string(), file_path, 100)
            .await;
        assert_eq!(
            manager.get_result("task1").unwrap().status,
            VerificationStatus::SizeMismatch
        );

        // Should still have only 1 result
        assert_eq!(manager.all_results().len(), 1);
    }

    #[tokio::test]
    async fn test_results_by_status_empty() {
        let manager = IntegrityManager::new();

        let verified = manager.results_by_status(VerificationStatus::Verified);
        assert!(verified.is_empty());

        let missing = manager.results_by_status(VerificationStatus::Missing);
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_tasks_needing_repair_empty() {
        let manager = IntegrityManager::new();
        let repair_needed = manager.tasks_needing_repair();
        assert!(repair_needed.is_empty());
    }

    #[tokio::test]
    async fn test_tasks_needing_repair_multiple() {
        let temp_dir = TempDir::new().unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        // file1 doesn't exist - Missing

        let file2 = temp_dir.path().join("file2.txt");
        tokio::fs::write(&file2, b"").await.unwrap();
        // file2 is empty - Empty

        let file3 = temp_dir.path().join("file3.txt");
        tokio::fs::write(&file3, b"content").await.unwrap();
        // file3 is correct - Verified

        let mut manager = IntegrityManager::new();
        manager
            .verify_file("task1".to_string(), "Task 1".to_string(), file1, 100)
            .await;
        manager
            .verify_file("task2".to_string(), "Task 2".to_string(), file2, 100)
            .await;
        manager
            .verify_file("task3".to_string(), "Task 3".to_string(), file3, 7)
            .await;

        let repair_needed = manager.tasks_needing_repair();
        assert_eq!(repair_needed.len(), 2);
    }

    // --- Summary tests ---
    #[tokio::test]
    async fn test_summary_empty_manager() {
        let manager = IntegrityManager::new();
        let summary = manager.summary();

        assert_eq!(summary.total_tasks, 0);
        assert_eq!(summary.verified, 0);
        assert!(!summary.all_verified());
        assert!(!summary.has_issues());
    }

    #[tokio::test]
    async fn test_summary_all_verified() {
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

        let summary = manager.summary();
        assert_eq!(summary.total_tasks, 2);
        assert_eq!(summary.verified, 2);
        assert!(summary.all_verified());
        assert!(!summary.has_issues());
    }

    // --- Persistence tests ---
    #[tokio::test]
    async fn test_save_config_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IntegrityManager::new();

        manager.save_config(temp_dir.path()).await.unwrap();

        let config_path = temp_dir.path().join("integrity_config.json");
        assert!(config_path.exists());
    }

    #[tokio::test]
    async fn test_save_config_overwrites() {
        let temp_dir = TempDir::new().unwrap();

        let mut manager = IntegrityManager::new();
        manager.set_config(IntegrityConfig {
            auto_verify_on_complete: true,
            periodic_verification: false,
            verification_interval_secs: 1000,
            only_verify_completed: true,
            max_batch_size: 10,
        });
        manager.save_config(temp_dir.path()).await.unwrap();

        manager.set_config(IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 9999,
            only_verify_completed: false,
            max_batch_size: 999,
        });
        manager.save_config(temp_dir.path()).await.unwrap();

        let loaded = IntegrityManager::load_config(temp_dir.path())
            .await
            .unwrap();
        assert!(!loaded.auto_verify_on_complete);
        assert!(loaded.periodic_verification);
        assert_eq!(loaded.verification_interval_secs, 9999);
        assert_eq!(loaded.max_batch_size, 999);
    }

    #[tokio::test]
    async fn test_save_config_no_tmp_leftover() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IntegrityManager::new();

        manager.save_config(temp_dir.path()).await.unwrap();

        let tmp_path = temp_dir.path().join("integrity_config.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_load_config_corrupt_json() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("integrity_config.json");
        tokio::fs::write(&config_path, "not valid json {{{")
            .await
            .unwrap();

        let result = IntegrityManager::load_config(temp_dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_config_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("integrity_config.json");
        tokio::fs::write(&config_path, "").await.unwrap();

        let result = IntegrityManager::load_config(temp_dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_load_config_roundtrip() {
        let temp_dir = TempDir::new().unwrap();

        let mut manager = IntegrityManager::new();
        manager.set_config(IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 12345,
            only_verify_completed: false,
            max_batch_size: 42,
        });

        manager.save_config(temp_dir.path()).await.unwrap();

        let loaded = IntegrityManager::load_config(temp_dir.path())
            .await
            .unwrap();

        assert_eq!(loaded.auto_verify_on_complete, false);
        assert_eq!(loaded.periodic_verification, true);
        assert_eq!(loaded.verification_interval_secs, 12345);
        assert_eq!(loaded.only_verify_completed, false);
        assert_eq!(loaded.max_batch_size, 42);
    }

    // --- Complete workflow tests ---
    #[tokio::test]
    async fn test_complete_workflow() {
        let temp_dir = TempDir::new().unwrap();

        // Create test files
        let file1 = temp_dir.path().join("download1.zip");
        tokio::fs::write(&file1, b"download content 1")
            .await
            .unwrap();

        let file2 = temp_dir.path().join("download2.zip");
        tokio::fs::write(&file2, b"download content 2 longer")
            .await
            .unwrap();

        let file3 = temp_dir.path().join("download3.zip");
        // file3 doesn't exist

        // Create manager with custom config
        let mut manager = IntegrityManager::with_config(IntegrityConfig {
            auto_verify_on_complete: true,
            periodic_verification: true,
            verification_interval_secs: 1800,
            only_verify_completed: true,
            max_batch_size: 10,
        });

        // Verify files
        let result1 = manager
            .verify_file("task1".to_string(), "Download 1".to_string(), file1, 18)
            .await;
        assert_eq!(result1.status, VerificationStatus::Verified);

        let result2 = manager
            .verify_file("task2".to_string(), "Download 2".to_string(), file2, 27)
            .await;
        assert_eq!(result2.status, VerificationStatus::Verified);

        let result3 = manager
            .verify_file("task3".to_string(), "Download 3".to_string(), file3, 100)
            .await;
        assert_eq!(result3.status, VerificationStatus::Missing);

        // Check summary
        let summary = manager.summary();
        assert_eq!(summary.total_tasks, 3);
        assert_eq!(summary.verified, 2);
        assert_eq!(summary.missing, 1);
        assert!(summary.has_issues());

        // Check repair needed
        let repair = manager.tasks_needing_repair();
        assert_eq!(repair.len(), 1);
        assert_eq!(repair[0].task_id, "task3");

        // Remove result
        assert!(manager.remove_result("task3"));
        assert_eq!(manager.summary().total_tasks, 2);

        // Save config
        manager.save_config(temp_dir.path()).await.unwrap();

        // Load config back
        let loaded_config = IntegrityManager::load_config(temp_dir.path())
            .await
            .unwrap();
        assert!(loaded_config.periodic_verification);
        assert_eq!(loaded_config.verification_interval_secs, 1800);
    }

    #[tokio::test]
    async fn test_multiple_verify_same_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello world").await.unwrap();

        let mut manager = IntegrityManager::new();

        // Verify multiple times
        for _ in 0..5 {
            manager
                .verify_file(
                    "task1".to_string(),
                    "Task".to_string(),
                    file_path.clone(),
                    11,
                )
                .await;
        }

        // Should still have only 1 result
        assert_eq!(manager.all_results().len(), 1);
        assert_eq!(
            manager.get_result("task1").unwrap().status,
            VerificationStatus::Verified
        );
    }

    // --- Error handling tests ---
    #[tokio::test]
    async fn test_verify_file_error_message() {
        let mut manager = IntegrityManager::new();

        // Try to verify a directory instead of a file (should cause an error on some systems)
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        let result = manager
            .verify_file(
                "task-dir".to_string(),
                "Directory Task".to_string(),
                dir_path,
                100,
            )
            .await;

        // Directory has metadata, so it will show as SizeMismatch or Verified depending on size
        // This tests that we handle directories gracefully
        assert!(
            result.error_message.is_none() || result.status == VerificationStatus::SizeMismatch
        );
    }

    #[tokio::test]
    async fn test_verify_file_preserves_all_fields() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::new();
        let result = manager
            .verify_file(
                "my-task-id".to_string(),
                "My Task Name".to_string(),
                file_path.clone(),
                5,
            )
            .await;

        assert_eq!(result.task_id, "my-task-id");
        assert_eq!(result.task_name, "My Task Name");
        assert_eq!(result.expected_size, 5);
        assert_eq!(result.actual_size, 5);
        assert_eq!(result.file_path, file_path);
        assert!(result.error_message.is_none());
    }

    // --- Edge cases ---
    #[test]
    fn test_summary_has_issues_only_mismatch() {
        let mut summary = IntegritySummary::new();
        let result = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task".to_string(),
            expected_size: 100,
            actual_size: 50,
            status: VerificationStatus::SizeMismatch,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&result);

        assert!(summary.has_issues());
    }

    #[test]
    fn test_summary_has_issues_only_errors() {
        let mut summary = IntegritySummary::new();
        let result = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task".to_string(),
            expected_size: 100,
            actual_size: 0,
            status: VerificationStatus::Error,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: Some("I/O error".to_string()),
        };
        summary.add_result(&result);

        assert!(summary.has_issues());
        assert_eq!(summary.errors, 1);
    }

    #[test]
    fn test_summary_has_issues_only_empty() {
        let mut summary = IntegritySummary::new();
        let result = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task".to_string(),
            expected_size: 100,
            actual_size: 0,
            status: VerificationStatus::Empty,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&result);

        assert!(summary.has_issues());
        assert_eq!(summary.empty, 1);
    }

    #[test]
    fn test_summary_pending_no_issues() {
        let mut summary = IntegritySummary::new();
        let result = VerificationResult {
            task_id: "1".to_string(),
            task_name: "Task".to_string(),
            expected_size: 100,
            actual_size: 0,
            status: VerificationStatus::Pending,
            file_path: PathBuf::from("/tmp/1"),
            verified_at: Utc::now(),
            error_message: None,
        };
        summary.add_result(&result);

        // Pending doesn't count as issue
        assert!(!summary.has_issues());
        assert_eq!(summary.pending, 1);
    }

    #[tokio::test]
    async fn test_clear_preserves_config() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let mut manager = IntegrityManager::with_config(IntegrityConfig {
            auto_verify_on_complete: false,
            periodic_verification: true,
            verification_interval_secs: 9999,
            only_verify_completed: false,
            max_batch_size: 50,
        });

        manager
            .verify_file("task1".to_string(), "Task".to_string(), file_path, 5)
            .await;

        // Clear results
        manager.clear();

        // Config should be preserved
        assert!(!manager.config().auto_verify_on_complete);
        assert!(manager.config().periodic_verification);
        assert_eq!(manager.config().verification_interval_secs, 9999);

        // Results should be empty
        assert!(manager.all_results().is_empty());
    }
}
