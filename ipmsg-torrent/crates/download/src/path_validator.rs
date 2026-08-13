//! Download Save Path Validation and Auto-Creation
//!
//! Validates save paths before downloads start to prevent:
//! - Path traversal attacks (e.g., `../../etc/passwd`)
//! - Invalid characters and reserved names
//! - Missing or unwritable directories
//!
//! Features:
//! - Path traversal detection using canonicalization
//! - Configurable base directory restrictions
//! - Auto-creation of missing directory structure
//! - Detailed validation results with actionable errors
//! - Integration with DownloadManager for automatic validation

use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::debug;

/// Errors from path validation
#[derive(Error, Debug)]
pub enum PathValidationError {
    #[error("Path traversal detected: {0}")]
    PathTraversal(String),

    #[error("Invalid character in path: {0}")]
    InvalidCharacter(String),

    #[error("Reserved name in path: {0}")]
    ReservedName(String),

    #[error("Path is too long (max {max} chars): {path}")]
    TooLong { path: String, max: usize },

    #[error("Empty path component")]
    EmptyComponent,

    #[error("Path is absolute but should be relative to base")]
    AbsolutePath,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Directory not writable: {0}")]
    NotWritable(PathBuf),

    #[error("Failed to create directory: {0}")]
    CreateFailed(PathBuf),
}

/// Result of path validation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the path is valid
    pub is_valid: bool,
    /// Canonical (absolute) path if valid
    pub canonical_path: Option<PathBuf>,
    /// List of warnings (non-fatal issues)
    pub warnings: Vec<String>,
    /// Error message if invalid
    pub error: Option<String>,
}

impl ValidationResult {
    /// Create a valid result
    pub fn valid(canonical: PathBuf) -> Self {
        Self {
            is_valid: true,
            canonical_path: Some(canonical),
            warnings: Vec::new(),
            error: None,
        }
    }

    /// Create an invalid result
    pub fn invalid(error: impl Into<String>) -> Self {
        Self {
            is_valid: false,
            canonical_path: None,
            warnings: Vec::new(),
            error: Some(error.into()),
        }
    }

    /// Add a warning
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

/// Configuration for path validation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PathValidatorConfig {
    /// Base directory that all paths must be under
    pub base_dir: PathBuf,

    /// Whether to auto-create missing directories
    pub auto_create_dirs: bool,

    /// Maximum path length (default: 4096)
    pub max_path_length: usize,

    /// Whether to check for reserved names (Windows compatibility)
    pub check_reserved_names: bool,

    /// Whether to allow absolute paths (default: false)
    pub allow_absolute_paths: bool,
}

impl Default for PathValidatorConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("."),
            auto_create_dirs: true,
            max_path_length: 4096,
            check_reserved_names: true,
            allow_absolute_paths: false,
        }
    }
}

/// Path validator for checking save paths
pub struct PathValidator {
    config: PathValidatorConfig,
}

impl Default for PathValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl PathValidator {
    /// Create a new path validator with default configuration
    pub fn new() -> Self {
        Self {
            config: PathValidatorConfig::default(),
        }
    }

    /// Create a path validator with custom configuration
    pub fn with_config(config: PathValidatorConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration
    pub fn config(&self) -> &PathValidatorConfig {
        &self.config
    }

    /// Update the base directory
    pub fn set_base_dir(&mut self, base_dir: PathBuf) {
        self.config.base_dir = base_dir;
    }

    /// Validate a save path
    ///
    /// This performs comprehensive validation:
    /// 1. Check for path traversal attempts
    /// 2. Validate characters and naming
    /// 3. Check path length
    /// 4. Verify the path is under the base directory
    /// 5. Optionally auto-create missing directories
    ///
    /// # Arguments
    ///
    /// * `path` - The path to validate (can be relative or absolute)
    ///
    /// # Returns
    ///
    /// A `ValidationResult` with the canonical path if valid, or error details if invalid
    pub async fn validate(&self, path: impl AsRef<Path>) -> ValidationResult {
        let path = path.as_ref();

        debug!("Validating path: {:?}", path);

        // Check for empty path
        if path.as_os_str().is_empty() {
            return ValidationResult::invalid("Path is empty");
        }

        // Check for absolute paths if not allowed
        if path.is_absolute() && !self.config.allow_absolute_paths {
            return ValidationResult::invalid("Absolute paths are not allowed");
        }

        // Check path length
        let path_str = path.to_string_lossy();
        if path_str.len() > self.config.max_path_length {
            return ValidationResult::invalid(format!(
                "Path is too long ({} chars, max {})",
                path_str.len(),
                self.config.max_path_length
            ));
        }

        // Check each component
        for component in path.components() {
            let component_str = component.as_os_str().to_string_lossy();

            // Check for empty components
            if component_str.is_empty() {
                return ValidationResult::invalid("Empty path component");
            }

            // Check for path traversal
            if component_str == ".." {
                return ValidationResult::invalid(format!(
                    "Path traversal detected: '{}'",
                    component_str
                ));
            }

            // Check for invalid characters
            if let Some(invalid) = self.check_invalid_chars(&component_str) {
                return ValidationResult::invalid(format!("Invalid character: '{}'", invalid));
            }

            // Check for reserved names (Windows compatibility)
            if self.config.check_reserved_names
                && let Some(reserved) = self.check_reserved_name(&component_str)
            {
                return ValidationResult::invalid(format!("Reserved name: '{}'", reserved));
            }
        }

        // Construct the full path
        let full_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.config.base_dir.join(path)
        };

        // Check if path is under base directory (prevent traversal via symlinks)
        if let Err(e) = self.check_path_traversal(&full_path).await {
            return ValidationResult::invalid(e.to_string());
        }

        // Try to canonicalize the path
        let canonical = match tokio::fs::canonicalize(&full_path).await {
            Ok(p) => p,
            Err(_) => {
                // Path doesn't exist yet, try to create it
                if self.config.auto_create_dirs {
                    // First ensure the parent directory exists
                    if let Some(parent) = full_path.parent()
                        && !parent.exists()
                        && let Err(e) = self.auto_create_directory(parent).await
                    {
                        return ValidationResult::invalid(e.to_string());
                    }
                    // Now create the target directory
                    if let Err(e) = self.auto_create_directory(&full_path).await {
                        return ValidationResult::invalid(e.to_string());
                    }
                    // Try canonicalize again after creation
                    match tokio::fs::canonicalize(&full_path).await {
                        Ok(p) => p,
                        Err(e) => {
                            return ValidationResult::invalid(format!(
                                "Failed to canonicalize path: {}",
                                e
                            ));
                        }
                    }
                } else {
                    return ValidationResult::invalid(format!(
                        "Path does not exist: {:?}",
                        full_path
                    ));
                }
            }
        };

        // Verify canonical path is still under base directory
        if let Err(e) = self.check_path_traversal(&canonical).await {
            return ValidationResult::invalid(format!("Path traversal detected: {}", e));
        }

        // Check if the parent directory is writable
        let parent = canonical.parent().unwrap_or(&canonical);
        if !self.check_writable(parent).await {
            return ValidationResult::invalid(format!("Directory not writable: {:?}", parent));
        }

        let mut result = ValidationResult::valid(canonical);

        // Add warnings for potential issues
        if path_str.contains("..") {
            result = result.with_warning("Path contains '..' but resolved safely");
        }

        debug!("Path validation successful: {:?}", result.canonical_path);

        result
    }

    /// Check for invalid characters in a path component
    fn check_invalid_chars(&self, component: &str) -> Option<char> {
        // Check for characters that are invalid on most filesystems
        let invalid_chars = ['<', '>', ':', '"', '|', '?', '*', '\0'];

        for c in invalid_chars {
            if component.contains(c) {
                return Some(c);
            }
        }

        // Check for control characters
        component.chars().find(|&c| c.is_control())
    }

    /// Check for reserved names (Windows compatibility)
    fn check_reserved_name(&self, component: &str) -> Option<&str> {
        // Windows reserved names (case-insensitive)
        let reserved = [
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
            "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
        ];

        // Extract the name without extension
        let name = component.split('.').next().unwrap_or(component);
        let name_upper = name.to_uppercase();

        reserved.iter().find(|&r| name_upper == *r).map(|v| v as _)
    }

    /// Check if a path attempts to traverse outside the base directory
    async fn check_path_traversal(&self, path: &Path) -> Result<(), PathValidationError> {
        // Get canonical base directory
        let canonical_base = tokio::fs::canonicalize(&self.config.base_dir)
            .await
            .map_err(|e| {
                PathValidationError::Io(std::io::Error::other(format!(
                    "Failed to canonicalize base directory: {}",
                    e
                )))
            })?;

        // Try to canonicalize the path
        let canonical_path = match tokio::fs::canonicalize(path).await {
            Ok(p) => p,
            Err(_) => {
                // Path doesn't exist yet, check the parent
                if let Some(parent) = path.parent()
                    && parent.exists()
                {
                    let canonical_parent = tokio::fs::canonicalize(parent).await?;
                    if !canonical_parent.starts_with(&canonical_base) {
                        return Err(PathValidationError::PathTraversal(format!(
                            "Path {:?} is outside base directory {:?}",
                            path, self.config.base_dir
                        )));
                    }
                    return Ok(());
                }
                // If parent doesn't exist either, we'll create it later
                return Ok(());
            }
        };

        // Check if canonical path starts with canonical base
        if !canonical_path.starts_with(&canonical_base) {
            return Err(PathValidationError::PathTraversal(format!(
                "Path {:?} is outside base directory {:?}",
                path, self.config.base_dir
            )));
        }

        Ok(())
    }

    /// Auto-create a directory and all parent directories
    async fn auto_create_directory(&self, path: &Path) -> Result<(), PathValidationError> {
        debug!("Auto-creating directory: {:?}", path);

        // Create the directory and all parents
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|_| PathValidationError::CreateFailed(path.to_path_buf()))?;

        // Verify it was created
        if !path.exists() {
            return Err(PathValidationError::CreateFailed(path.to_path_buf()));
        }

        Ok(())
    }

    /// Check if a directory is writable
    async fn check_writable(&self, path: &Path) -> bool {
        // Try to create a temporary file
        let test_file = path.join(".ipmsg_write_test");
        match tokio::fs::write(&test_file, b"test").await {
            Ok(()) => {
                // Clean up
                let _ = tokio::fs::remove_file(&test_file).await;
                true
            }
            Err(_) => false,
        }
    }

    /// Validate multiple paths at once
    pub async fn validate_all(
        &self,
        paths: &[impl AsRef<Path>],
    ) -> Vec<(PathBuf, ValidationResult)> {
        let mut results = Vec::new();

        for path in paths {
            let path_buf = path.as_ref().to_path_buf();
            let result = self.validate(path).await;
            results.push((path_buf, result));
        }

        results
    }
}

/// Convenience function to validate a path with default settings
pub async fn validate_path(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> ValidationResult {
    let config = PathValidatorConfig {
        base_dir: base_dir.as_ref().to_path_buf(),
        ..Default::default()
    };
    let validator = PathValidator::with_config(config);
    validator.validate(path).await
}

/// Convenience function to validate and auto-create a path
pub async fn validate_and_create(
    path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> Result<PathBuf, PathValidationError> {
    let config = PathValidatorConfig {
        base_dir: base_dir.as_ref().to_path_buf(),
        auto_create_dirs: true,
        ..Default::default()
    };
    let validator = PathValidator::with_config(config);
    let result = validator.validate(path).await;

    if result.is_valid {
        Ok(result.canonical_path.unwrap())
    } else {
        Err(PathValidationError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            result
                .error
                .unwrap_or_else(|| "Validation failed".to_string()),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_validate_valid_relative_path() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("downloads/file.txt", temp_dir.path()).await;

        assert!(result.is_valid);
        assert!(result.canonical_path.is_some());
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn test_validate_path_traversal() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("../../etc/passwd", temp_dir.path()).await;

        assert!(!result.is_valid);
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("traversal"));
    }

    #[tokio::test]
    async fn test_validate_absolute_path_not_allowed() {
        let temp_dir = TempDir::new().unwrap();
        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            allow_absolute_paths: false,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);
        let result = validator.validate("/etc/passwd").await;

        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Absolute paths"));
    }

    #[tokio::test]
    async fn test_validate_absolute_path_allowed() {
        let temp_dir = TempDir::new().unwrap();
        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            allow_absolute_paths: true,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);

        // Create a test directory
        let test_dir = temp_dir.path().join("test");
        tokio::fs::create_dir_all(&test_dir).await.unwrap();

        let result = validator.validate(&test_dir).await;
        assert!(result.is_valid);
    }

    #[tokio::test]
    async fn test_validate_invalid_characters() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("file<name>.txt", temp_dir.path()).await;

        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Invalid character"));
    }

    #[tokio::test]
    async fn test_validate_reserved_name() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("CON", temp_dir.path()).await;

        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Reserved name"));
    }

    #[tokio::test]
    async fn test_validate_reserved_name_with_extension() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("CON.txt", temp_dir.path()).await;

        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Reserved name"));
    }

    #[tokio::test]
    async fn test_validate_path_too_long() {
        let temp_dir = TempDir::new().unwrap();
        let long_path = "a".repeat(5000);
        let result = validate_path(&long_path, temp_dir.path()).await;

        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("too long"));
    }

    #[tokio::test]
    async fn test_validate_empty_path() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("", temp_dir.path()).await;

        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("empty"));
    }

    #[tokio::test]
    async fn test_auto_create_directory() {
        let temp_dir = TempDir::new().unwrap();
        let relative_path = PathBuf::from("new").join("nested").join("dir");
        let absolute_path = temp_dir.path().join(&relative_path);

        // Use relative path with validate_path
        let result = validate_path(&relative_path, temp_dir.path()).await;

        assert!(result.is_valid, "Validation failed: {:?}", result.error);
        assert!(absolute_path.exists());
    }

    #[tokio::test]
    async fn test_validate_multiple_paths() {
        let temp_dir = TempDir::new().unwrap();
        let paths = vec!["file1.txt", "file2.txt", "../outside.txt"];

        let validator = PathValidator::with_config(PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        });

        let results = validator.validate_all(&paths).await;

        assert_eq!(results.len(), 3);
        assert!(results[0].1.is_valid);
        assert!(results[1].1.is_valid);
        assert!(!results[2].1.is_valid);
    }

    #[tokio::test]
    async fn test_validate_nested_path() {
        let temp_dir = TempDir::new().unwrap();
        let nested = PathBuf::from("level1").join("level2");

        let result = validate_path(&nested, temp_dir.path()).await;

        assert!(result.is_valid, "Validation failed: {:?}", result.error);
        assert!(temp_dir.path().join(&nested).exists());
    }

    #[test]
    fn test_check_invalid_chars() {
        let validator = PathValidator::new();

        assert!(validator.check_invalid_chars("normal.txt").is_none());
        assert_eq!(validator.check_invalid_chars("file<name"), Some('<'));
        assert_eq!(validator.check_invalid_chars("file>name"), Some('>'));
        assert_eq!(validator.check_invalid_chars("file:name"), Some(':'));
        assert_eq!(validator.check_invalid_chars("file|name"), Some('|'));
        assert_eq!(validator.check_invalid_chars("file?name"), Some('?'));
        assert_eq!(validator.check_invalid_chars("file*name"), Some('*'));
    }

    #[test]
    fn test_check_reserved_name() {
        let validator = PathValidator::new();

        assert!(validator.check_reserved_name("normal.txt").is_none());
        assert_eq!(validator.check_reserved_name("CON"), Some("CON"));
        assert_eq!(validator.check_reserved_name("con"), Some("CON"));
        assert_eq!(validator.check_reserved_name("CON.txt"), Some("CON"));
        assert_eq!(validator.check_reserved_name("PRN"), Some("PRN"));
        assert_eq!(validator.check_reserved_name("NUL"), Some("NUL"));
        assert_eq!(validator.check_reserved_name("COM1"), Some("COM1"));
        assert_eq!(validator.check_reserved_name("LPT1"), Some("LPT1"));
    }

    #[tokio::test]
    async fn test_validation_result_builder() {
        let result =
            ValidationResult::valid(PathBuf::from("/test/path")).with_warning("Test warning");

        assert!(result.is_valid);
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0], "Test warning");
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = PathValidatorConfig {
            base_dir: PathBuf::from("/base"),
            auto_create_dirs: false,
            max_path_length: 1000,
            check_reserved_names: false,
            allow_absolute_paths: true,
        };

        let validator = PathValidator::with_config(config);
        assert_eq!(validator.config().max_path_length, 1000);
        assert!(!validator.config().auto_create_dirs);
    }

    // ===== PathValidationError Display =====

    #[test]
    fn test_error_display_path_traversal() {
        let err = PathValidationError::PathTraversal("../../etc".into());
        assert_eq!(err.to_string(), "Path traversal detected: ../../etc");
    }

    #[test]
    fn test_error_display_invalid_character() {
        let err = PathValidationError::InvalidCharacter("<".into());
        assert_eq!(err.to_string(), "Invalid character in path: <");
    }

    #[test]
    fn test_error_display_reserved_name() {
        let err = PathValidationError::ReservedName("CON".into());
        assert_eq!(err.to_string(), "Reserved name in path: CON");
    }

    #[test]
    fn test_error_display_too_long() {
        let err = PathValidationError::TooLong {
            path: "very/long/path".into(),
            max: 100,
        };
        let msg = err.to_string();
        assert!(msg.contains("too long"));
        assert!(msg.contains("100"));
        assert!(msg.contains("very/long/path"));
    }

    #[test]
    fn test_error_display_empty_component() {
        let err = PathValidationError::EmptyComponent;
        assert_eq!(err.to_string(), "Empty path component");
    }

    #[test]
    fn test_error_display_absolute_path() {
        let err = PathValidationError::AbsolutePath;
        assert_eq!(
            err.to_string(),
            "Path is absolute but should be relative to base"
        );
    }

    #[test]
    fn test_error_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let err = PathValidationError::Io(io_err);
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_error_display_not_writable() {
        let err = PathValidationError::NotWritable(PathBuf::from("/readonly"));
        let msg = err.to_string();
        assert!(msg.contains("not writable"));
        assert!(msg.contains("/readonly"));
    }

    #[test]
    fn test_error_display_create_failed() {
        let err = PathValidationError::CreateFailed(PathBuf::from("/some/dir"));
        let msg = err.to_string();
        assert!(msg.contains("Failed to create directory"));
        assert!(msg.contains("/some/dir"));
    }

    // ===== PathValidationError From<io::Error> =====

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let path_err: PathValidationError = io_err.into();
        assert!(path_err.to_string().contains("denied"));
    }

    // ===== PathValidationError Debug =====

    #[test]
    fn test_error_debug_variants() {
        // Verify Debug impl for all variants
        let errors: Vec<Box<dyn std::fmt::Debug>> = vec![
            Box::new(PathValidationError::PathTraversal("test".into())),
            Box::new(PathValidationError::InvalidCharacter("x".into())),
            Box::new(PathValidationError::ReservedName("CON".into())),
            Box::new(PathValidationError::TooLong {
                path: "p".into(),
                max: 10,
            }),
            Box::new(PathValidationError::EmptyComponent),
            Box::new(PathValidationError::AbsolutePath),
            Box::new(PathValidationError::Io(std::io::Error::other("io"))),
            Box::new(PathValidationError::NotWritable(PathBuf::from("/x"))),
            Box::new(PathValidationError::CreateFailed(PathBuf::from("/y"))),
        ];
        for err in &errors {
            let debug_str = format!("{:?}", err);
            assert!(!debug_str.is_empty());
        }
    }

    // ===== ValidationResult =====

    #[test]
    fn test_validation_result_invalid() {
        let result = ValidationResult::invalid("some error");
        assert!(!result.is_valid);
        assert!(result.canonical_path.is_none());
        assert!(result.warnings.is_empty());
        assert_eq!(result.error.as_deref(), Some("some error"));
    }

    #[test]
    fn test_validation_result_valid_no_warnings() {
        let result = ValidationResult::valid(PathBuf::from("/ok"));
        assert!(result.is_valid);
        assert_eq!(result.canonical_path, Some(PathBuf::from("/ok")));
        assert!(result.warnings.is_empty());
        assert!(result.error.is_none());
    }

    #[test]
    fn test_validation_result_multiple_warnings() {
        let result = ValidationResult::valid(PathBuf::from("/ok"))
            .with_warning("warn1")
            .with_warning("warn2")
            .with_warning("warn3");
        assert_eq!(result.warnings.len(), 3);
        assert_eq!(result.warnings[0], "warn1");
        assert_eq!(result.warnings[2], "warn3");
    }

    #[test]
    fn test_validation_result_clone() {
        let result = ValidationResult::valid(PathBuf::from("/ok")).with_warning("test");
        let cloned = result.clone();
        assert_eq!(cloned.is_valid, result.is_valid);
        assert_eq!(cloned.canonical_path, result.canonical_path);
        assert_eq!(cloned.warnings, result.warnings);
        assert_eq!(cloned.error, result.error);
    }

    // ===== PathValidatorConfig =====

    #[test]
    fn test_config_default_values() {
        let config = PathValidatorConfig::default();
        assert_eq!(config.base_dir, PathBuf::from("."));
        assert!(config.auto_create_dirs);
        assert_eq!(config.max_path_length, 4096);
        assert!(config.check_reserved_names);
        assert!(!config.allow_absolute_paths);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = PathValidatorConfig {
            base_dir: PathBuf::from("/tmp/downloads"),
            auto_create_dirs: false,
            max_path_length: 2048,
            check_reserved_names: false,
            allow_absolute_paths: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: PathValidatorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.base_dir, config.base_dir);
        assert_eq!(loaded.auto_create_dirs, config.auto_create_dirs);
        assert_eq!(loaded.max_path_length, config.max_path_length);
        assert_eq!(loaded.check_reserved_names, config.check_reserved_names);
        assert_eq!(loaded.allow_absolute_paths, config.allow_absolute_paths);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{
            "base_dir": "/tmp",
            "auto_create_dirs": true,
            "max_path_length": 4096,
            "check_reserved_names": true,
            "allow_absolute_paths": false,
            "unknown_field": 42
        }"#;
        let config: PathValidatorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.base_dir, PathBuf::from("/tmp"));
    }

    // ===== PathValidator Default =====

    #[test]
    fn test_validator_default_trait() {
        let validator = PathValidator::default();
        assert_eq!(validator.config().base_dir, PathBuf::from("."));
        assert!(validator.config().auto_create_dirs);
    }

    // ===== set_base_dir =====

    #[test]
    fn test_set_base_dir() {
        let mut validator = PathValidator::new();
        assert_eq!(validator.config().base_dir, PathBuf::from("."));
        validator.set_base_dir(PathBuf::from("/new/base"));
        assert_eq!(validator.config().base_dir, PathBuf::from("/new/base"));
    }

    // ===== check_invalid_chars extended =====

    #[test]
    fn test_check_invalid_chars_null_byte() {
        let validator = PathValidator::new();
        assert_eq!(validator.check_invalid_chars("file\0name"), Some('\0'));
    }

    #[test]
    fn test_check_invalid_chars_control_chars() {
        let validator = PathValidator::new();
        // Tab is a control character
        assert_eq!(validator.check_invalid_chars("file\tname"), Some('\t'));
        // Newline is a control character
        assert_eq!(validator.check_invalid_chars("file\nname"), Some('\n'));
    }

    #[test]
    fn test_check_invalid_chars_normal() {
        let validator = PathValidator::new();
        assert!(
            validator
                .check_invalid_chars("hello_world-123.txt")
                .is_none()
        );
        assert!(validator.check_invalid_chars("日本語ファイル").is_none());
        assert!(validator.check_invalid_chars("файл").is_none());
        assert!(validator.check_invalid_chars("café").is_none());
    }

    // ===== check_reserved_name extended =====

    #[test]
    fn test_check_reserved_name_all_com_ports() {
        let validator = PathValidator::new();
        for i in 1..=9 {
            let name = format!("COM{}", i);
            // Just verify it returns Some for all COM ports
            assert!(validator.check_reserved_name(&name).is_some());
        }
    }

    #[test]
    fn test_check_reserved_name_all_lpt_ports() {
        let validator = PathValidator::new();
        for i in 1..=9 {
            let name = format!("LPT{}", i);
            assert!(validator.check_reserved_name(&name).is_some());
        }
    }

    #[test]
    fn test_check_reserved_name_case_insensitive() {
        let validator = PathValidator::new();
        assert!(validator.check_reserved_name("con").is_some());
        assert!(validator.check_reserved_name("Con").is_some());
        assert!(validator.check_reserved_name("cOn").is_some());
        assert!(validator.check_reserved_name("nul").is_some());
        assert!(validator.check_reserved_name("Nul").is_some());
        assert!(validator.check_reserved_name("aux").is_some());
        assert!(validator.check_reserved_name("prn").is_some());
    }

    #[test]
    fn test_check_reserved_name_non_reserved() {
        let validator = PathValidator::new();
        assert!(validator.check_reserved_name("myfile").is_none());
        assert!(validator.check_reserved_name("config").is_none());
        assert!(validator.check_reserved_name("data").is_none());
        assert!(validator.check_reserved_name("COM0").is_none()); // COM0 not reserved
        assert!(validator.check_reserved_name("LPT0").is_none()); // LPT0 not reserved
        assert!(validator.check_reserved_name("COM10").is_none()); // COM10 not reserved
    }

    #[test]
    fn test_check_reserved_name_with_multiple_dots() {
        let validator = PathValidator::new();
        // "CON" is extracted from first split on '.'
        assert!(validator.check_reserved_name("CON.tar.gz").is_some());
        assert!(validator.check_reserved_name("NUL.dat.bak").is_some());
    }

    // ===== Validation boundary: exact max_path_length =====

    #[tokio::test]
    async fn test_validate_path_exact_max_length() {
        let temp_dir = TempDir::new().unwrap();
        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_path_length: 10,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);

        // Exactly 10 chars should be ok
        let result = validator.validate("aaaaaaaaaa").await;
        // It may fail for other reasons (path doesn't exist, auto-create), but NOT for length
        if !result.is_valid {
            assert!(!result.error.as_ref().unwrap().contains("too long"));
        }
    }

    #[tokio::test]
    async fn test_validate_path_one_over_max_length() {
        let temp_dir = TempDir::new().unwrap();
        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_path_length: 10,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);

        // 11 chars should fail
        let result = validator.validate("aaaaaaaaaaa").await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("too long"));
    }

    // ===== auto_create_dirs = false =====

    #[tokio::test]
    async fn test_validate_nonexistent_no_auto_create() {
        let temp_dir = TempDir::new().unwrap();
        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            auto_create_dirs: false,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);

        let result = validator.validate("nonexistent_dir").await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_validate_existing_dir_no_auto_create() {
        let temp_dir = TempDir::new().unwrap();
        let sub = temp_dir.path().join("existing");
        tokio::fs::create_dir_all(&sub).await.unwrap();

        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            auto_create_dirs: false,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);

        let result = validator.validate("existing").await;
        assert!(result.is_valid);
    }

    // ===== check_reserved_names = false =====

    #[tokio::test]
    async fn test_validate_reserved_name_check_disabled() {
        let temp_dir = TempDir::new().unwrap();
        // Create the CON directory so it exists
        let con_dir = temp_dir.path().join("CON");
        tokio::fs::create_dir_all(&con_dir).await.unwrap();

        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            check_reserved_names: false,
            ..Default::default()
        };
        let validator = PathValidator::with_config(config);

        let result = validator.validate("CON").await;
        // Should not fail on reserved name check
        assert!(result.is_valid);
    }

    // ===== validate_and_create =====

    #[tokio::test]
    async fn test_validate_and_create_success() {
        let temp_dir = TempDir::new().unwrap();

        let result = validate_and_create("new_dir", temp_dir.path()).await;
        assert!(
            result.is_ok(),
            "validate_and_create failed: {:?}",
            result.err()
        );
        assert!(temp_dir.path().join("new_dir").exists());
    }

    #[tokio::test]
    async fn test_validate_and_create_failure() {
        let temp_dir = TempDir::new().unwrap();
        // Path with invalid characters should fail
        let bad_path = temp_dir.path().join("file<bad>name");

        let result = validate_and_create(&bad_path, temp_dir.path()).await;
        assert!(result.is_err());
    }

    // ===== validate_all edge cases =====

    #[tokio::test]
    async fn test_validate_all_empty_slice() {
        let temp_dir = TempDir::new().unwrap();
        let validator = PathValidator::with_config(PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        });

        let paths: Vec<&str> = vec![];
        let results = validator.validate_all(&paths).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_validate_all_single_valid() {
        let temp_dir = TempDir::new().unwrap();
        let validator = PathValidator::with_config(PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        });

        let paths = vec!["single_file.txt"];
        let results = validator.validate_all(&paths).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.is_valid);
    }

    #[tokio::test]
    async fn test_validate_all_preserves_order() {
        let temp_dir = TempDir::new().unwrap();
        let validator = PathValidator::with_config(PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        });

        let paths = vec!["a.txt", "b.txt", "c.txt"];
        let results = validator.validate_all(&paths).await;
        assert_eq!(results[0].0, PathBuf::from("a.txt"));
        assert_eq!(results[1].0, PathBuf::from("b.txt"));
        assert_eq!(results[2].0, PathBuf::from("c.txt"));
    }

    // ===== Unicode path handling =====

    #[tokio::test]
    async fn test_validate_unicode_path() {
        let temp_dir = TempDir::new().unwrap();
        let unicode_dir = temp_dir.path().join("日本語");
        tokio::fs::create_dir_all(&unicode_dir).await.unwrap();

        let result = validate_path("日本語", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    #[tokio::test]
    async fn test_validate_emoji_path() {
        let temp_dir = TempDir::new().unwrap();
        let emoji_dir = temp_dir.path().join("📁downloads");
        tokio::fs::create_dir_all(&emoji_dir).await.unwrap();

        let result = validate_path("📁downloads", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    #[tokio::test]
    async fn test_validate_cyrillic_path() {
        let temp_dir = TempDir::new().unwrap();
        let cyrillic_dir = temp_dir.path().join("файлы");
        tokio::fs::create_dir_all(&cyrillic_dir).await.unwrap();

        let result = validate_path("файлы", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    // ===== Path with spaces and dots =====

    #[tokio::test]
    async fn test_validate_path_with_spaces() {
        let temp_dir = TempDir::new().unwrap();
        let spaced_dir = temp_dir.path().join("my downloads");
        tokio::fs::create_dir_all(&spaced_dir).await.unwrap();

        let result = validate_path("my downloads", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    #[tokio::test]
    async fn test_validate_path_with_dots_in_name() {
        let temp_dir = TempDir::new().unwrap();
        let dotted_dir = temp_dir.path().join("v1.2.3");
        tokio::fs::create_dir_all(&dotted_dir).await.unwrap();

        let result = validate_path("v1.2.3", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    // ===== Validation: path is a file not directory =====

    #[tokio::test]
    async fn test_validate_existing_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("existing_file.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        // Validating a file (not directory) - should succeed as path validation
        // checks the path itself, not whether it's a directory
        let result = validate_path("existing_file.txt", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    // ===== Writable check =====

    #[tokio::test]
    async fn test_validate_writable_directory() {
        let temp_dir = TempDir::new().unwrap();
        let sub = temp_dir.path().join("writable_dir");
        tokio::fs::create_dir_all(&sub).await.unwrap();

        let result = validate_path("writable_dir", temp_dir.path()).await;
        assert!(result.is_valid);
        // The temp dir should be writable on any normal system
    }

    // ===== Convenience functions =====

    #[tokio::test]
    async fn test_validate_path_convenience() {
        let temp_dir = TempDir::new().unwrap();
        let sub = temp_dir.path().join("conv");
        tokio::fs::create_dir_all(&sub).await.unwrap();

        let result = validate_path("conv", &temp_dir).await;
        assert!(result.is_valid);
    }

    // ===== Multiple invalid characters in same component =====

    #[tokio::test]
    async fn test_validate_multiple_invalid_chars() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("file<>name", temp_dir.path()).await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Invalid character"));
    }

    // ===== Path traversal variants =====

    #[tokio::test]
    async fn test_validate_dot_dot_in_middle() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("sub/../../outside", temp_dir.path()).await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("traversal"));
    }

    #[tokio::test]
    async fn test_validate_dot_dot_at_end() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("sub/..", temp_dir.path()).await;
        // "sub/.." contains ".." component
        assert!(!result.is_valid);
    }

    // ===== Reserved names: AUX, COM2-9, LPT2-9 =====

    #[tokio::test]
    async fn test_validate_reserved_aux() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("AUX", temp_dir.path()).await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Reserved name"));
    }

    #[tokio::test]
    async fn test_validate_reserved_com2() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("COM2", temp_dir.path()).await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Reserved name"));
    }

    #[tokio::test]
    async fn test_validate_reserved_lpt9() {
        let temp_dir = TempDir::new().unwrap();
        let result = validate_path("LPT9", temp_dir.path()).await;
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("Reserved name"));
    }

    // ===== Non-reserved similar names =====

    #[tokio::test]
    async fn test_validate_non_reserved_similar() {
        let temp_dir = TempDir::new().unwrap();
        let sub = temp_dir.path().join("CONF");
        tokio::fs::create_dir_all(&sub).await.unwrap();

        let result = validate_path("CONF", temp_dir.path()).await;
        assert!(result.is_valid);
    }

    // ===== Deeply nested auto-create =====

    #[tokio::test]
    async fn test_auto_create_deeply_nested() {
        let temp_dir = TempDir::new().unwrap();
        let deep = PathBuf::from("a").join("b").join("c").join("d").join("e");

        let result = validate_and_create(&deep, temp_dir.path()).await;
        assert!(
            result.is_ok(),
            "validate_and_create failed: {:?}",
            result.err()
        );
        assert!(temp_dir.path().join(&deep).exists());
    }

    // ===== validate with existing path that's already canonical =====

    #[tokio::test]
    async fn test_validate_canonical_path_returned() {
        let temp_dir = TempDir::new().unwrap();
        let sub = temp_dir.path().join("canonical_test");
        tokio::fs::create_dir_all(&sub).await.unwrap();

        let result = validate_path("canonical_test", temp_dir.path()).await;
        assert!(result.is_valid);
        let canonical = result.canonical_path.unwrap();
        assert!(canonical.is_absolute());
        // Should be under the temp dir
        assert!(canonical.starts_with(temp_dir.path()));
    }

    // ===== validate_all with all invalid =====

    #[tokio::test]
    async fn test_validate_all_all_invalid() {
        let temp_dir = TempDir::new().unwrap();
        let validator = PathValidator::with_config(PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        });

        let paths = vec!["../a", "../b", "../c"];
        let results = validator.validate_all(&paths).await;
        assert_eq!(results.len(), 3);
        for (_, result) in &results {
            assert!(!result.is_valid);
        }
    }

    // ===== Empty component detection =====

    #[test]
    fn test_check_invalid_chars_empty_string() {
        let validator = PathValidator::new();
        // Empty string has no invalid chars
        assert!(validator.check_invalid_chars("").is_none());
    }

    // ===== Reserved name edge cases =====

    #[test]
    fn test_check_reserved_name_dot_only() {
        let validator = PathValidator::new();
        // "." splits to "" which is not reserved
        assert!(validator.check_reserved_name(".").is_none());
    }

    #[test]
    fn test_check_reserved_name_starts_with_dot() {
        let validator = PathValidator::new();
        // ".hidden" - name before first dot is empty
        assert!(validator.check_reserved_name(".hidden").is_none());
    }

    // ===== Full workflow =====

    #[tokio::test]
    async fn test_full_validation_workflow() {
        let temp_dir = TempDir::new().unwrap();

        // 1. Create validator with custom config
        let config = PathValidatorConfig {
            base_dir: temp_dir.path().to_path_buf(),
            auto_create_dirs: true,
            max_path_length: 4096,
            check_reserved_names: true,
            allow_absolute_paths: false,
        };
        let mut validator = PathValidator::with_config(config);

        // 2. Validate a valid path
        let result = validator.validate("downloads/movies").await;
        assert!(result.is_valid);
        assert!(result.canonical_path.is_some());

        // 3. Change base dir
        let new_base = temp_dir.path().join("new_base");
        tokio::fs::create_dir_all(&new_base).await.unwrap();
        validator.set_base_dir(new_base.clone());

        // 4. Validate under new base
        let result = validator.validate("subdir").await;
        assert!(result.is_valid);

        // 5. Try invalid path
        let result = validator.validate("../../etc").await;
        assert!(!result.is_valid);

        // 6. Validate multiple
        let results = validator.validate_all(&["ok1", "ok2"]).await;
        assert_eq!(results.len(), 2);
        assert!(results[0].1.is_valid);
        assert!(results[1].1.is_valid);
    }
}
