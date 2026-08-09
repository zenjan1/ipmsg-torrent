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
            if self.config.check_reserved_names {
                if let Some(reserved) = self.check_reserved_name(&component_str) {
                    return ValidationResult::invalid(format!("Reserved name: '{}'", reserved));
                }
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
                    if let Some(parent) = full_path.parent() {
                        if !parent.exists() {
                            if let Err(e) = self.auto_create_directory(parent).await {
                                return ValidationResult::invalid(e.to_string());
                            }
                        }
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
        for c in component.chars() {
            if c.is_control() {
                return Some(c);
            }
        }

        None
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

        for r in &reserved {
            if name_upper == *r {
                return Some(r);
            }
        }

        None
    }

    /// Check if a path attempts to traverse outside the base directory
    async fn check_path_traversal(&self, path: &Path) -> Result<(), PathValidationError> {
        // Get canonical base directory
        let canonical_base = tokio::fs::canonicalize(&self.config.base_dir)
            .await
            .map_err(|e| {
                PathValidationError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to canonicalize base directory: {}", e),
                ))
            })?;

        // Try to canonicalize the path
        let canonical_path = match tokio::fs::canonicalize(path).await {
            Ok(p) => p,
            Err(_) => {
                // Path doesn't exist yet, check the parent
                if let Some(parent) = path.parent() {
                    if parent.exists() {
                        let canonical_parent = tokio::fs::canonicalize(parent).await?;
                        if !canonical_parent.starts_with(&canonical_base) {
                            return Err(PathValidationError::PathTraversal(format!(
                                "Path {:?} is outside base directory {:?}",
                                path, self.config.base_dir
                            )));
                        }
                        return Ok(());
                    }
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
        let new_dir = temp_dir.path().join("new").join("nested").join("dir");

        let result = validate_and_create(&new_dir, temp_dir.path()).await;

        assert!(result.is_ok());
        assert!(new_dir.exists());
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
        let nested = temp_dir
            .path()
            .join("level1")
            .join("level2")
            .join("file.txt");

        let result = validate_and_create(nested.parent().unwrap(), temp_dir.path()).await;

        assert!(result.is_ok());
        assert!(nested.parent().unwrap().exists());
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
}
