//! Disk space monitoring and pre-check for download manager.
//!
//! Provides:
//! - Pre-download disk space validation
//! - Background monitoring during active downloads
//! - Automatic pause when disk space is critically low
//! - Automatic resume when space is freed

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Result of a disk space check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskSpaceStatus {
    /// Plenty of space available (> 2x safety margin)
    Sufficient,
    /// Space is low but above critical threshold
    Low,
    /// Space is critically low (< safety margin)
    Critical,
}

impl std::fmt::Display for DiskSpaceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskSpaceStatus::Sufficient => write!(f, "Sufficient"),
            DiskSpaceStatus::Low => write!(f, "Low"),
            DiskSpaceStatus::Critical => write!(f, "Critical"),
        }
    }
}

/// Configuration for disk space monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMonitorConfig {
    /// Whether disk monitoring is enabled
    pub enabled: bool,
    /// Safety margin in bytes (space to keep free)
    pub safety_margin_bytes: u64,
    /// Check interval in seconds
    pub check_interval_secs: u64,
    /// Automatically pause downloads when disk space is critical
    pub auto_pause_on_critical: bool,
    /// Automatically resume downloads when disk space recovers
    pub auto_resume_on_recovery: bool,
}

impl Default for DiskMonitorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            safety_margin_bytes: 100_000_000, // 100MB
            check_interval_secs: 30,
            auto_pause_on_critical: true,
            auto_resume_on_recovery: true,
        }
    }
}

/// Summary of disk monitor state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMonitorSummary {
    pub enabled: bool,
    pub status: DiskSpaceStatus,
    pub available_bytes: u64,
    pub total_bytes: u64,
    pub warning_threshold_bytes: u64,
    pub critical_threshold_bytes: u64,
    pub safety_margin_bytes: u64,
    pub check_interval_secs: u64,
    pub is_monitoring: bool,
    pub auto_pause_on_critical: bool,
    pub auto_resume_on_recovery: bool,
    pub auto_paused_count: u32,
    pub auto_resumed_count: u32,
}

/// Save disk monitor configuration to disk (atomic write).
pub async fn save_disk_monitor_config(
    config: &DiskMonitorConfig,
    data_dir: &Path,
) -> std::io::Result<()> {
    let path = data_dir.join("disk_monitor_config.json");
    let tmp_path = data_dir.join("disk_monitor_config.json.tmp");
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;
    tokio::fs::write(&tmp_path, json.as_bytes()).await?;
    tokio::fs::rename(&tmp_path, &path).await?;
    Ok(())
}

/// Load disk monitor configuration from disk.
pub async fn load_disk_monitor_config(data_dir: &Path) -> Option<DiskMonitorConfig> {
    let path = data_dir.join("disk_monitor_config.json");
    let data = tokio::fs::read_to_string(&path).await.ok()?;
    serde_json::from_str(&data).ok()
}

/// Error type for disk space operations.
#[derive(Debug, thiserror::Error)]
pub enum DiskSpaceError {
    #[error("Failed to query disk space for path: {0}")]
    QueryFailed(String),
    #[error("Insufficient disk space: need {needed} bytes, have {available} bytes")]
    Insufficient { needed: u64, available: u64 },
}

/// Get available disk space (in bytes) for the filesystem containing `path`.
///
/// Uses `statvfs` on Unix systems. Returns the space available to non-privileged users.
pub fn get_available_space(path: &Path) -> Result<u64, DiskSpaceError> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        // statvfs is the POSIX standard for filesystem statistics
        let path_str = path.to_string_lossy();
        let c_path = CString::new(path_str.as_ref())
            .map_err(|_| DiskSpaceError::QueryFailed("invalid path".into()))?;

        let mut stat = MaybeUninit::<libc::statvfs>::uninit();

        // SAFETY: stat is a valid pointer, c_path is a valid C string
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };

        if ret != 0 {
            return Err(DiskSpaceError::QueryFailed(format!(
                "statvfs failed for {}",
                path_str
            )));
        }

        let stat = unsafe { stat.assume_init() };
        // f_bavail is available blocks for non-privileged users
        // f_frsize is the fundamental filesystem block size
        Ok(stat.f_bavail * stat.f_frsize)
    }

    #[cfg(not(unix))]
    {
        // Fallback: assume plenty of space on non-Unix
        let _ = path;
        Ok(u64::MAX)
    }
}

/// Get total disk space (in bytes) for the filesystem containing `path`.
///
/// Uses `statvfs` on Unix systems. Returns the total space of the filesystem.
pub fn get_total_space(path: &Path) -> Result<u64, DiskSpaceError> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        let path_str = path.to_string_lossy();
        let c_path = CString::new(path_str.as_ref())
            .map_err(|_| DiskSpaceError::QueryFailed("invalid path".into()))?;

        let mut stat = MaybeUninit::<libc::statvfs>::uninit();
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };

        if ret != 0 {
            return Err(DiskSpaceError::QueryFailed(format!(
                "statvfs failed for {}",
                path_str
            )));
        }

        let stat = unsafe { stat.assume_init() };
        // f_blocks is total number of blocks, f_frsize is fundamental block size
        Ok(stat.f_blocks * stat.f_frsize)
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}

/// Check if there is enough disk space for a download.
///
/// # Arguments
/// * `path` - Directory where the file will be saved
/// * `required_bytes` - Expected file size in bytes
/// * `safety_margin_bytes` - Extra space to keep free (default 100MB)
///
/// # Returns
/// * `Ok(())` if enough space is available
/// * `Err(DiskSpaceError::Insufficient)` if not enough space
pub fn check_disk_space(
    path: &Path,
    required_bytes: u64,
    safety_margin_bytes: u64,
) -> Result<(), DiskSpaceError> {
    let available = get_available_space(path)?;
    let needed = required_bytes + safety_margin_bytes;

    if available >= needed {
        Ok(())
    } else {
        Err(DiskSpaceError::Insufficient { needed, available })
    }
}

/// Background disk space monitor.
///
/// Periodically checks disk space and can trigger automatic pause/resume
/// of downloads when space becomes critical or is freed.
pub struct DiskSpaceMonitor {
    /// Path to monitor (typically the download directory)
    monitor_path: PathBuf,
    /// Safety margin in bytes (space to keep free)
    safety_margin_bytes: u64,
    /// Check interval in seconds
    check_interval_secs: u64,
    /// Current status (updated by background task)
    status: Arc<Mutex<DiskSpaceStatus>>,
    /// Whether the monitor is currently active
    running: Arc<Mutex<bool>>,
    /// Cancellation token for the background task
    cancel_token: Arc<Mutex<Option<CancellationToken>>>,
    /// Count of auto-paused events
    auto_paused_count: Arc<Mutex<u32>>,
    /// Count of auto-resumed events
    auto_resumed_count: Arc<Mutex<u32>>,
}

impl DiskSpaceMonitor {
    /// Create a new disk space monitor.
    ///
    /// # Arguments
    /// * `monitor_path` - Directory to monitor
    /// * `safety_margin_bytes` - Minimum free space to maintain (bytes)
    /// * `check_interval_secs` - How often to check (seconds)
    pub fn new(monitor_path: PathBuf, safety_margin_bytes: u64, check_interval_secs: u64) -> Self {
        Self {
            monitor_path,
            safety_margin_bytes,
            check_interval_secs,
            status: Arc::new(Mutex::new(DiskSpaceStatus::Sufficient)),
            running: Arc::new(Mutex::new(false)),
            cancel_token: Arc::new(Mutex::new(None)),
            auto_paused_count: Arc::new(Mutex::new(0)),
            auto_resumed_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Get the current disk space status.
    pub async fn get_status(&self) -> DiskSpaceStatus {
        *self.status.lock().await
    }

    /// Check disk space and update status.
    pub async fn check(&self) -> DiskSpaceStatus {
        let status = match get_available_space(&self.monitor_path) {
            Ok(available) => {
                if available < self.safety_margin_bytes {
                    DiskSpaceStatus::Critical
                } else if available < self.safety_margin_bytes * 2 {
                    DiskSpaceStatus::Low
                } else {
                    DiskSpaceStatus::Sufficient
                }
            }
            Err(_) => DiskSpaceStatus::Sufficient, // Assume OK if we can't check
        };

        *self.status.lock().await = status;
        status
    }

    /// Start background monitoring.
    ///
    /// The monitor will check disk space every `check_interval_secs` seconds.
    /// When space becomes critical, `on_critical` callback is called.
    /// When space recovers from critical, `on_recover` callback is called.
    pub async fn start_monitoring<FC, FR, FutC, FutR>(&self, on_critical: FC, on_recover: FR)
    where
        FC: Fn() -> FutC + Send + Sync + 'static,
        FR: Fn() -> FutR + Send + Sync + 'static,
        FutC: std::future::Future<Output = ()> + Send,
        FutR: std::future::Future<Output = ()> + Send,
    {
        let mut running = self.running.lock().await;
        if *running {
            return;
        }
        *running = true;

        let cancel_token = CancellationToken::new();
        *self.cancel_token.lock().await = Some(cancel_token.clone());

        let monitor_path = self.monitor_path.clone();
        let safety_margin = self.safety_margin_bytes;
        let interval_secs = self.check_interval_secs;
        let status = self.status.clone();
        let running_flag = self.running.clone();

        tokio::spawn(async move {
            let mut was_critical = false;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let current_status = match get_available_space(&monitor_path) {
                            Ok(available) => {
                                if available < safety_margin {
                                    DiskSpaceStatus::Critical
                                } else if available < safety_margin * 2 {
                                    DiskSpaceStatus::Low
                                } else {
                                    DiskSpaceStatus::Sufficient
                                }
                            }
                            Err(_) => DiskSpaceStatus::Sufficient,
                        };

                        *status.lock().await = current_status;

                        let is_critical = current_status == DiskSpaceStatus::Critical;

                        if is_critical && !was_critical {
                            on_critical().await;
                        } else if !is_critical && was_critical {
                            on_recover().await;
                        }

                        was_critical = is_critical;
                    }
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                }
            }

            *running_flag.lock().await = false;
        });
    }

    /// Stop background monitoring.
    pub async fn stop_monitoring(&self) {
        let mut running = self.running.lock().await;
        if !*running {
            return;
        }

        if let Some(token) = self.cancel_token.lock().await.take() {
            token.cancel();
        }
        *running = false;
    }

    /// Check if the monitor is currently running.
    pub async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    /// Get the path being monitored.
    pub fn monitor_path(&self) -> &Path {
        &self.monitor_path
    }

    /// Get the configured safety margin in bytes.
    pub fn safety_margin_bytes(&self) -> u64 {
        self.safety_margin_bytes
    }

    /// Get the configured check interval in seconds.
    pub fn check_interval_secs(&self) -> u64 {
        self.check_interval_secs
    }

    /// Get the auto-paused count.
    pub async fn auto_paused_count(&self) -> u32 {
        *self.auto_paused_count.lock().await
    }

    /// Get the auto-resumed count.
    pub async fn auto_resumed_count(&self) -> u32 {
        *self.auto_resumed_count.lock().await
    }

    /// Increment the auto-paused counter.
    pub async fn increment_auto_paused(&self) {
        let mut count = self.auto_paused_count.lock().await;
        *count += 1;
    }

    /// Increment the auto-resumed counter.
    pub async fn increment_auto_resumed(&self) {
        let mut count = self.auto_resumed_count.lock().await;
        *count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // ===== DiskSpaceStatus tests =====

    #[test]
    fn test_disk_space_status_eq() {
        assert_eq!(DiskSpaceStatus::Sufficient, DiskSpaceStatus::Sufficient);
        assert_eq!(DiskSpaceStatus::Low, DiskSpaceStatus::Low);
        assert_eq!(DiskSpaceStatus::Critical, DiskSpaceStatus::Critical);
        assert_ne!(DiskSpaceStatus::Sufficient, DiskSpaceStatus::Critical);
        assert_ne!(DiskSpaceStatus::Sufficient, DiskSpaceStatus::Low);
        assert_ne!(DiskSpaceStatus::Low, DiskSpaceStatus::Critical);
    }

    #[test]
    fn test_disk_space_status_display() {
        assert_eq!(format!("{}", DiskSpaceStatus::Sufficient), "Sufficient");
        assert_eq!(format!("{}", DiskSpaceStatus::Low), "Low");
        assert_eq!(format!("{}", DiskSpaceStatus::Critical), "Critical");
    }

    #[test]
    fn test_disk_space_status_clone_copy() {
        let status = DiskSpaceStatus::Sufficient;
        let cloned = status;
        let copied = status;
        assert_eq!(cloned, status);
        assert_eq!(copied, status);
    }

    #[test]
    fn test_disk_space_status_debug() {
        let debug = format!("{:?}", DiskSpaceStatus::Sufficient);
        assert_eq!(debug, "Sufficient");
        let debug = format!("{:?}", DiskSpaceStatus::Low);
        assert_eq!(debug, "Low");
        let debug = format!("{:?}", DiskSpaceStatus::Critical);
        assert_eq!(debug, "Critical");
    }

    #[test]
    fn test_disk_space_status_serde_roundtrip() {
        for status in [
            DiskSpaceStatus::Sufficient,
            DiskSpaceStatus::Low,
            DiskSpaceStatus::Critical,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: DiskSpaceStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn test_disk_space_status_serde_snake_case() {
        // Verify serde uses the variant names as-is (no rename)
        let json = serde_json::to_string(&DiskSpaceStatus::Sufficient).unwrap();
        assert_eq!(json, "\"Sufficient\"");
        let json = serde_json::to_string(&DiskSpaceStatus::Low).unwrap();
        assert_eq!(json, "\"Low\"");
        let json = serde_json::to_string(&DiskSpaceStatus::Critical).unwrap();
        assert_eq!(json, "\"Critical\"");
    }

    // ===== DiskMonitorConfig tests =====

    #[test]
    fn test_disk_monitor_config_default() {
        let config = DiskMonitorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.safety_margin_bytes, 100_000_000);
        assert_eq!(config.check_interval_secs, 30);
        assert!(config.auto_pause_on_critical);
        assert!(config.auto_resume_on_recovery);
    }

    #[test]
    fn test_disk_monitor_config_custom_values() {
        let config = DiskMonitorConfig {
            enabled: false,
            safety_margin_bytes: 500_000_000,
            check_interval_secs: 60,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: false,
        };
        assert!(!config.enabled);
        assert_eq!(config.safety_margin_bytes, 500_000_000);
        assert_eq!(config.check_interval_secs, 60);
        assert!(!config.auto_pause_on_critical);
        assert!(!config.auto_resume_on_recovery);
    }

    #[test]
    fn test_disk_monitor_config_clone() {
        let config = DiskMonitorConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.safety_margin_bytes, config.safety_margin_bytes);
        assert_eq!(cloned.check_interval_secs, config.check_interval_secs);
        assert_eq!(cloned.auto_pause_on_critical, config.auto_pause_on_critical);
        assert_eq!(
            cloned.auto_resume_on_recovery,
            config.auto_resume_on_recovery
        );
    }

    #[test]
    fn test_disk_monitor_config_debug() {
        let config = DiskMonitorConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("enabled"));
        assert!(debug.contains("safety_margin_bytes"));
        assert!(debug.contains("100000000"));
    }

    #[test]
    fn test_disk_monitor_config_serde_roundtrip() {
        let config = DiskMonitorConfig {
            enabled: false,
            safety_margin_bytes: 250_000_000,
            check_interval_secs: 45,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DiskMonitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.safety_margin_bytes, 250_000_000);
        assert_eq!(deserialized.check_interval_secs, 45);
        assert_eq!(deserialized.auto_pause_on_critical, false);
        assert_eq!(deserialized.auto_resume_on_recovery, true);
    }

    #[test]
    fn test_disk_monitor_config_pretty_serde() {
        let config = DiskMonitorConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let deserialized: DiskMonitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
    }

    #[test]
    fn test_disk_monitor_config_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "safety_margin_bytes": 100000000,
            "check_interval_secs": 30,
            "auto_pause_on_critical": true,
            "auto_resume_on_recovery": true,
            "unknown_field": "should be ignored",
            "another_extra": 42
        }"#;
        let config: DiskMonitorConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.safety_margin_bytes, 100_000_000);
    }

    // ===== DiskMonitorSummary tests =====

    #[test]
    fn test_disk_monitor_summary_serde_roundtrip() {
        let summary = DiskMonitorSummary {
            enabled: true,
            status: DiskSpaceStatus::Sufficient,
            available_bytes: 1_000_000_000,
            total_bytes: 10_000_000_000,
            warning_threshold_bytes: 2_000_000_000,
            critical_threshold_bytes: 1_000_000_000,
            safety_margin_bytes: 100_000_000,
            check_interval_secs: 30,
            is_monitoring: false,
            auto_pause_on_critical: true,
            auto_resume_on_recovery: true,
            auto_paused_count: 0,
            auto_resumed_count: 0,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: DiskMonitorSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.status, DiskSpaceStatus::Sufficient);
        assert_eq!(deserialized.available_bytes, 1_000_000_000);
        assert_eq!(deserialized.total_bytes, 10_000_000_000);
        assert_eq!(deserialized.auto_paused_count, 0);
        assert_eq!(deserialized.auto_resumed_count, 0);
    }

    #[test]
    fn test_disk_monitor_summary_all_status_variants() {
        for status in [
            DiskSpaceStatus::Sufficient,
            DiskSpaceStatus::Low,
            DiskSpaceStatus::Critical,
        ] {
            let summary = DiskMonitorSummary {
                enabled: true,
                status,
                available_bytes: 500_000_000,
                total_bytes: 1_000_000_000,
                warning_threshold_bytes: 200_000_000,
                critical_threshold_bytes: 100_000_000,
                safety_margin_bytes: 100_000_000,
                check_interval_secs: 30,
                is_monitoring: true,
                auto_pause_on_critical: true,
                auto_resume_on_recovery: true,
                auto_paused_count: 5,
                auto_resumed_count: 3,
            };
            let json = serde_json::to_string(&summary).unwrap();
            let deserialized: DiskMonitorSummary = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.status, status);
        }
    }

    #[test]
    fn test_disk_monitor_summary_clone() {
        let summary = DiskMonitorSummary {
            enabled: true,
            status: DiskSpaceStatus::Low,
            available_bytes: 100_000_000,
            total_bytes: 500_000_000,
            warning_threshold_bytes: 200_000_000,
            critical_threshold_bytes: 100_000_000,
            safety_margin_bytes: 50_000_000,
            check_interval_secs: 15,
            is_monitoring: true,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: false,
            auto_paused_count: 10,
            auto_resumed_count: 5,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.enabled, summary.enabled);
        assert_eq!(cloned.status, summary.status);
        assert_eq!(cloned.available_bytes, summary.available_bytes);
        assert_eq!(cloned.auto_paused_count, summary.auto_paused_count);
    }

    #[test]
    fn test_disk_monitor_summary_debug() {
        let summary = DiskMonitorSummary {
            enabled: true,
            status: DiskSpaceStatus::Critical,
            available_bytes: 50_000_000,
            total_bytes: 1_000_000_000,
            warning_threshold_bytes: 200_000_000,
            critical_threshold_bytes: 100_000_000,
            safety_margin_bytes: 100_000_000,
            check_interval_secs: 30,
            is_monitoring: true,
            auto_pause_on_critical: true,
            auto_resume_on_recovery: true,
            auto_paused_count: 3,
            auto_resumed_count: 1,
        };
        let debug = format!("{:?}", summary);
        assert!(debug.contains("Critical"));
        assert!(debug.contains("50000000"));
        assert!(debug.contains("auto_paused_count: 3"));
    }

    #[test]
    fn test_disk_monitor_summary_zero_values() {
        let summary = DiskMonitorSummary {
            enabled: false,
            status: DiskSpaceStatus::Sufficient,
            available_bytes: 0,
            total_bytes: 0,
            warning_threshold_bytes: 0,
            critical_threshold_bytes: 0,
            safety_margin_bytes: 0,
            check_interval_secs: 0,
            is_monitoring: false,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: false,
            auto_paused_count: 0,
            auto_resumed_count: 0,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: DiskMonitorSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.available_bytes, 0);
        assert_eq!(deserialized.total_bytes, 0);
    }

    #[test]
    fn test_disk_monitor_summary_large_values() {
        let summary = DiskMonitorSummary {
            enabled: true,
            status: DiskSpaceStatus::Sufficient,
            available_bytes: u64::MAX,
            total_bytes: u64::MAX,
            warning_threshold_bytes: u64::MAX,
            critical_threshold_bytes: u64::MAX,
            safety_margin_bytes: u64::MAX,
            check_interval_secs: u64::MAX,
            is_monitoring: true,
            auto_pause_on_critical: true,
            auto_resume_on_recovery: true,
            auto_paused_count: u32::MAX,
            auto_resumed_count: u32::MAX,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: DiskMonitorSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.available_bytes, u64::MAX);
        assert_eq!(deserialized.auto_paused_count, u32::MAX);
    }

    // ===== DiskSpaceError tests =====

    #[test]
    fn test_disk_space_error_query_failed_display() {
        let error = DiskSpaceError::QueryFailed("test path".to_string());
        let display = format!("{}", error);
        assert!(display.contains("Failed to query disk space"));
        assert!(display.contains("test path"));
    }

    #[test]
    fn test_disk_space_error_insufficient_display() {
        let error = DiskSpaceError::Insufficient {
            needed: 1000,
            available: 500,
        };
        let display = format!("{}", error);
        assert!(display.contains("Insufficient disk space"));
        assert!(display.contains("1000"));
        assert!(display.contains("500"));
    }

    #[test]
    fn test_disk_space_error_debug() {
        let error = DiskSpaceError::QueryFailed("path".to_string());
        let debug = format!("{:?}", error);
        assert!(debug.contains("QueryFailed"));

        let error = DiskSpaceError::Insufficient {
            needed: 100,
            available: 50,
        };
        let debug = format!("{:?}", error);
        assert!(debug.contains("Insufficient"));
    }

    #[test]
    fn test_disk_space_error_is_error_trait() {
        let error: Box<dyn std::error::Error> =
            Box::new(DiskSpaceError::QueryFailed("test".to_string()));
        assert!(error.to_string().contains("Failed to query"));

        let error: Box<dyn std::error::Error> = Box::new(DiskSpaceError::Insufficient {
            needed: 100,
            available: 50,
        });
        assert!(error.to_string().contains("Insufficient"));
    }

    // ===== get_available_space tests =====

    #[test]
    fn test_get_available_space_tmp() {
        let space = get_available_space(Path::new("/tmp"));
        assert!(space.is_ok());
        let bytes = space.unwrap();
        assert!(
            bytes > 1024 * 1024,
            "Expected >1MB free, got {} bytes",
            bytes
        );
    }

    #[test]
    fn test_get_available_space_nonexistent_path() {
        let result = get_available_space(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
    }

    #[test]
    fn test_get_available_space_root() {
        let space = get_available_space(Path::new("/"));
        assert!(space.is_ok());
        assert!(space.unwrap() > 0);
    }

    #[test]
    fn test_get_available_space_home() {
        if let Ok(home) = std::env::var("HOME") {
            let space = get_available_space(Path::new(&home));
            assert!(space.is_ok());
        }
    }

    #[test]
    fn test_get_available_space_error_message() {
        let result = get_available_space(Path::new("/nonexistent/xyz"));
        match result {
            Err(DiskSpaceError::QueryFailed(msg)) => {
                assert!(msg.contains("statvfs failed"));
            }
            _ => panic!("Expected QueryFailed error"),
        }
    }

    // ===== get_total_space tests =====

    #[test]
    fn test_get_total_space_tmp() {
        let space = get_total_space(Path::new("/tmp"));
        assert!(space.is_ok());
        let bytes = space.unwrap();
        // Total should be at least 1GB on any modern system
        assert!(
            bytes > 1_000_000_000,
            "Expected >1GB total, got {} bytes",
            bytes
        );
    }

    #[test]
    fn test_get_total_space_nonexistent_path() {
        let result = get_total_space(Path::new("/nonexistent/path/xyz"));
        assert!(result.is_err());
    }

    #[test]
    fn test_get_total_space_root() {
        let space = get_total_space(Path::new("/"));
        assert!(space.is_ok());
        assert!(space.unwrap() > 0);
    }

    #[test]
    fn test_get_total_space_greater_than_available() {
        let total = get_total_space(Path::new("/tmp")).unwrap();
        let available = get_available_space(Path::new("/tmp")).unwrap();
        // Total should always be >= available
        assert!(total >= available);
    }

    // ===== check_disk_space tests =====

    #[test]
    fn test_check_disk_space_sufficient() {
        let result = check_disk_space(Path::new("/tmp"), 1024, 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_disk_space_insufficient() {
        let result = check_disk_space(Path::new("/tmp"), u64::MAX / 2, u64::MAX / 2);
        assert!(result.is_err());
        match result.unwrap_err() {
            DiskSpaceError::Insufficient { needed, available } => {
                assert!(needed > available);
            }
            _ => panic!("Expected Insufficient error"),
        }
    }

    #[test]
    fn test_check_disk_space_zero_required() {
        // Zero bytes required should always succeed
        let result = check_disk_space(Path::new("/tmp"), 0, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_disk_space_zero_safety_margin() {
        // Zero safety margin, small requirement
        let result = check_disk_space(Path::new("/tmp"), 1024, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_disk_space_large_safety_margin() {
        // Large safety margin should fail
        let result = check_disk_space(Path::new("/tmp"), 0, u64::MAX);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_disk_space_invalid_path() {
        let result = check_disk_space(Path::new("/nonexistent/path"), 1024, 1024);
        assert!(result.is_err());
        match result.unwrap_err() {
            DiskSpaceError::QueryFailed(_) => {}
            _ => panic!("Expected QueryFailed error"),
        }
    }

    #[test]
    fn test_check_disk_space_error_values() {
        // Use large but non-overflowing values
        let result = check_disk_space(Path::new("/tmp"), 1_000_000_000_000, 1_000_000_000_000);
        match result {
            Err(DiskSpaceError::Insufficient { needed, available }) => {
                assert_eq!(needed, 2_000_000_000_000);
                assert!(available > 0);
                assert!(needed > available);
            }
            _ => panic!("Expected Insufficient error"),
        }
    }

    // ===== DiskSpaceMonitor tests =====

    #[test]
    fn test_disk_monitor_new() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp/downloads"), 100_000_000, 30);
        assert_eq!(monitor.monitor_path(), Path::new("/tmp/downloads"));
        assert_eq!(monitor.safety_margin_bytes(), 100_000_000);
        assert_eq!(monitor.check_interval_secs(), 30);
    }

    #[test]
    fn test_disk_monitor_new_zero_values() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 0, 0);
        assert_eq!(monitor.safety_margin_bytes(), 0);
        assert_eq!(monitor.check_interval_secs(), 0);
    }

    #[test]
    fn test_disk_monitor_new_large_values() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), u64::MAX, u64::MAX);
        assert_eq!(monitor.safety_margin_bytes(), u64::MAX);
        assert_eq!(monitor.check_interval_secs(), u64::MAX);
    }

    #[tokio::test]
    async fn test_disk_monitor_initial_status() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 100_000_000, 30);
        // Initial status should be Sufficient
        let status = monitor.get_status().await;
        assert_eq!(status, DiskSpaceStatus::Sufficient);
    }

    #[tokio::test]
    async fn test_disk_monitor_check() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Sufficient);
    }

    #[tokio::test]
    async fn test_disk_monitor_check_updates_status() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        let status = monitor.check().await;
        // get_status should return the same value after check
        let stored = monitor.get_status().await;
        assert_eq!(stored, status);
    }

    #[tokio::test]
    async fn test_disk_monitor_check_invalid_path_returns_sufficient() {
        // Invalid path should fallback to Sufficient
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/nonexistent/path"), 1024, 5);
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Sufficient);
    }

    #[tokio::test]
    async fn test_disk_monitor_check_with_large_margin_critical() {
        // With a huge safety margin, even /tmp should be critical
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), u64::MAX, 5);
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Critical);
    }

    #[tokio::test]
    async fn test_disk_monitor_start_stop() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024 * 1024, 1);

        assert!(!monitor.is_running().await);

        monitor.start_monitoring(|| async {}, || async {}).await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(monitor.is_running().await);

        monitor.stop_monitoring().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!monitor.is_running().await);
    }

    #[tokio::test]
    async fn test_disk_monitor_double_start() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024 * 1024, 1);

        monitor.start_monitoring(|| async {}, || async {}).await;
        monitor.start_monitoring(|| async {}, || async {}).await;

        assert!(monitor.is_running().await);
        monitor.stop_monitoring().await;
    }

    #[tokio::test]
    async fn test_disk_monitor_stop_when_not_running() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024 * 1024, 1);
        assert!(!monitor.is_running().await);
        // Stop should be a no-op when not running
        monitor.stop_monitoring().await;
        assert!(!monitor.is_running().await);
    }

    #[tokio::test]
    async fn test_disk_monitor_properties() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp/dl"), 500_000_000, 30);
        assert_eq!(monitor.monitor_path(), Path::new("/tmp/dl"));
        assert_eq!(monitor.safety_margin_bytes(), 500_000_000);
        assert_eq!(monitor.check_interval_secs(), 30);
    }

    // ===== Auto counter tests =====

    #[tokio::test]
    async fn test_disk_monitor_auto_paused_count_initial() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        assert_eq!(monitor.auto_paused_count().await, 0);
    }

    #[tokio::test]
    async fn test_disk_monitor_auto_resumed_count_initial() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        assert_eq!(monitor.auto_resumed_count().await, 0);
    }

    #[tokio::test]
    async fn test_disk_monitor_increment_auto_paused() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        assert_eq!(monitor.auto_paused_count().await, 0);
        monitor.increment_auto_paused().await;
        assert_eq!(monitor.auto_paused_count().await, 1);
        monitor.increment_auto_paused().await;
        assert_eq!(monitor.auto_paused_count().await, 2);
    }

    #[tokio::test]
    async fn test_disk_monitor_increment_auto_resumed() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        assert_eq!(monitor.auto_resumed_count().await, 0);
        monitor.increment_auto_resumed().await;
        assert_eq!(monitor.auto_resumed_count().await, 1);
        monitor.increment_auto_resumed().await;
        assert_eq!(monitor.auto_resumed_count().await, 2);
    }

    #[tokio::test]
    async fn test_disk_monitor_counters_independent() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        monitor.increment_auto_paused().await;
        monitor.increment_auto_paused().await;
        monitor.increment_auto_resumed().await;
        assert_eq!(monitor.auto_paused_count().await, 2);
        assert_eq!(monitor.auto_resumed_count().await, 1);
    }

    #[tokio::test]
    async fn test_disk_monitor_counters_large_values() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        for _ in 0..100 {
            monitor.increment_auto_paused().await;
            monitor.increment_auto_resumed().await;
        }
        assert_eq!(monitor.auto_paused_count().await, 100);
        assert_eq!(monitor.auto_resumed_count().await, 100);
    }

    // ===== Persistence tests =====

    #[tokio::test]
    async fn test_config_save_load() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_config");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let config = DiskMonitorConfig {
            enabled: false,
            safety_margin_bytes: 50_000_000,
            check_interval_secs: 60,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: false,
        };

        save_disk_monitor_config(&config, &tmp_dir).await.unwrap();
        let loaded = load_disk_monitor_config(&tmp_dir).await.unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.safety_margin_bytes, 50_000_000);
        assert_eq!(loaded.check_interval_secs, 60);
        assert_eq!(loaded.auto_pause_on_critical, false);
        assert_eq!(loaded.auto_resume_on_recovery, false);

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_load_missing_file() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_missing");
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        let result = load_disk_monitor_config(&tmp_dir).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_config_save_creates_file() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_creates");
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let config = DiskMonitorConfig::default();
        save_disk_monitor_config(&config, &tmp_dir).await.unwrap();

        let file_path = tmp_dir.join("disk_monitor_config.json");
        assert!(file_path.exists());

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_save_overwrites() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_overwrite");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let config1 = DiskMonitorConfig {
            enabled: true,
            safety_margin_bytes: 100_000_000,
            check_interval_secs: 30,
            auto_pause_on_critical: true,
            auto_resume_on_recovery: true,
        };
        save_disk_monitor_config(&config1, &tmp_dir).await.unwrap();

        let config2 = DiskMonitorConfig {
            enabled: false,
            safety_margin_bytes: 200_000_000,
            check_interval_secs: 60,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: false,
        };
        save_disk_monitor_config(&config2, &tmp_dir).await.unwrap();

        let loaded = load_disk_monitor_config(&tmp_dir).await.unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.safety_margin_bytes, 200_000_000);

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_save_no_tmp_leftover() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_no_tmp");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let config = DiskMonitorConfig::default();
        save_disk_monitor_config(&config, &tmp_dir).await.unwrap();

        let tmp_file = tmp_dir.join("disk_monitor_config.json.tmp");
        assert!(!tmp_file.exists());

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_load_corrupt_json() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_corrupt");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let file_path = tmp_dir.join("disk_monitor_config.json");
        tokio::fs::write(&file_path, "not valid json {{{")
            .await
            .unwrap();

        let result = load_disk_monitor_config(&tmp_dir).await;
        assert!(result.is_none());

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_load_empty_file() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_empty");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let file_path = tmp_dir.join("disk_monitor_config.json");
        tokio::fs::write(&file_path, "").await.unwrap();

        let result = load_disk_monitor_config(&tmp_dir).await;
        assert!(result.is_none());

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_save_pretty_json() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_pretty");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let config = DiskMonitorConfig::default();
        save_disk_monitor_config(&config, &tmp_dir).await.unwrap();

        let file_path = tmp_dir.join("disk_monitor_config.json");
        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        // Pretty JSON should have newlines
        assert!(content.contains('\n'));

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_config_roundtrip_all_fields() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_test_roundtrip");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        let config = DiskMonitorConfig {
            enabled: true,
            safety_margin_bytes: 123_456_789,
            check_interval_secs: 42,
            auto_pause_on_critical: false,
            auto_resume_on_recovery: true,
        };

        save_disk_monitor_config(&config, &tmp_dir).await.unwrap();
        let loaded = load_disk_monitor_config(&tmp_dir).await.unwrap();

        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.safety_margin_bytes, config.safety_margin_bytes);
        assert_eq!(loaded.check_interval_secs, config.check_interval_secs);
        assert_eq!(loaded.auto_pause_on_critical, config.auto_pause_on_critical);
        assert_eq!(
            loaded.auto_resume_on_recovery,
            config.auto_resume_on_recovery
        );

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    // ===== Threshold boundary tests =====

    #[tokio::test]
    async fn test_disk_monitor_threshold_sufficient() {
        // /tmp with tiny margin should be Sufficient
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Sufficient);
    }

    #[tokio::test]
    async fn test_disk_monitor_threshold_critical() {
        // With u64::MAX margin, should be Critical
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), u64::MAX, 5);
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Critical);
    }

    #[tokio::test]
    async fn test_disk_monitor_threshold_zero_margin() {
        // With zero margin, any space is Sufficient
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 0, 5);
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Sufficient);
    }

    // ===== Unicode path tests =====

    #[test]
    fn test_disk_monitor_unicode_path() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp/下载目录"), 1024, 5);
        assert_eq!(monitor.monitor_path(), Path::new("/tmp/下载目录"));
    }

    #[test]
    fn test_disk_monitor_emoji_path() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp/📁downloads"), 1024, 5);
        assert_eq!(monitor.monitor_path(), Path::new("/tmp/📁downloads"));
    }

    // ===== Integration-style tests =====

    #[tokio::test]
    async fn test_disk_monitor_full_lifecycle() {
        let tmp_dir = std::env::temp_dir().join("disk_monitor_lifecycle");
        let _ = tokio::fs::create_dir_all(&tmp_dir).await;

        // Create monitor
        let monitor = DiskSpaceMonitor::new(tmp_dir.clone(), 1024, 1);

        // Check initial state
        assert!(!monitor.is_running().await);
        assert_eq!(monitor.get_status().await, DiskSpaceStatus::Sufficient);
        assert_eq!(monitor.auto_paused_count().await, 0);
        assert_eq!(monitor.auto_resumed_count().await, 0);

        // Start monitoring
        monitor.start_monitoring(|| async {}, || async {}).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(monitor.is_running().await);

        // Check status
        let status = monitor.check().await;
        assert_eq!(status, DiskSpaceStatus::Sufficient);

        // Increment counters
        monitor.increment_auto_paused().await;
        monitor.increment_auto_resumed().await;
        assert_eq!(monitor.auto_paused_count().await, 1);
        assert_eq!(monitor.auto_resumed_count().await, 1);

        // Stop monitoring
        monitor.stop_monitoring().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!monitor.is_running().await);

        // Save and load config
        let config = DiskMonitorConfig::default();
        save_disk_monitor_config(&config, &tmp_dir).await.unwrap();
        let loaded = load_disk_monitor_config(&tmp_dir).await.unwrap();
        assert_eq!(loaded.enabled, true);

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_disk_monitor_multiple_check_calls() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);

        // Multiple checks should all return consistent results
        let status1 = monitor.check().await;
        let status2 = monitor.check().await;
        let status3 = monitor.check().await;

        assert_eq!(status1, status2);
        assert_eq!(status2, status3);
    }

    #[tokio::test]
    async fn test_disk_monitor_stop_idempotent() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 1);

        // Stop without starting should be fine
        monitor.stop_monitoring().await;
        monitor.stop_monitoring().await;
        assert!(!monitor.is_running().await);
    }

    #[tokio::test]
    async fn test_disk_monitor_restart_after_stop() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024 * 1024, 1);

        // Start
        monitor.start_monitoring(|| async {}, || async {}).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(monitor.is_running().await);

        // Stop
        monitor.stop_monitoring().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!monitor.is_running().await);

        // Restart
        monitor.start_monitoring(|| async {}, || async {}).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(monitor.is_running().await);

        monitor.stop_monitoring().await;
    }
}
