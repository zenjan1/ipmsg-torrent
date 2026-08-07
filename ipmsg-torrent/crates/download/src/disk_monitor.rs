//! Disk space monitoring and pre-check for download manager.
//!
//! Provides:
//! - Pre-download disk space validation
//! - Background monitoring during active downloads
//! - Automatic pause when disk space is critically low
//! - Automatic resume when space is freed

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Result of a disk space check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskSpaceStatus {
    /// Plenty of space available (> 2x safety margin)
    Sufficient,
    /// Space is low but above critical threshold
    Low,
    /// Space is critically low (< safety margin)
    Critical,
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
        Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
    }

    #[cfg(not(unix))]
    {
        // Fallback: assume plenty of space on non-Unix
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
        Err(DiskSpaceError::Insufficient {
            needed,
            available,
        })
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
}

impl DiskSpaceMonitor {
    /// Create a new disk space monitor.
    ///
    /// # Arguments
    /// * `monitor_path` - Directory to monitor
    /// * `safety_margin_bytes` - Minimum free space to maintain (bytes)
    /// * `check_interval_secs` - How often to check (seconds)
    pub fn new(
        monitor_path: PathBuf,
        safety_margin_bytes: u64,
        check_interval_secs: u64,
    ) -> Self {
        Self {
            monitor_path,
            safety_margin_bytes,
            check_interval_secs,
            status: Arc::new(Mutex::new(DiskSpaceStatus::Sufficient)),
            running: Arc::new(Mutex::new(false)),
            cancel_token: Arc::new(Mutex::new(None)),
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
    pub async fn start_monitoring<FC, FR, FutC, FutR>(
        &self,
        on_critical: FC,
        on_recover: FR,
    ) where
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
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(interval_secs));

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_get_available_space_tmp() {
        // /tmp should always exist and have some space
        let space = get_available_space(Path::new("/tmp"));
        assert!(space.is_ok());
        let bytes = space.unwrap();
        // Should have at least 1MB free (very conservative)
        assert!(bytes > 1024 * 1024, "Expected >1MB free, got {} bytes", bytes);
    }

    #[test]
    fn test_get_available_space_nonexistent_path() {
        // Non-existent path should fail on statvfs
        let result = get_available_space(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
    }

    #[test]
    fn test_check_disk_space_sufficient() {
        // Check with a tiny requirement on /tmp
        let result = check_disk_space(Path::new("/tmp"), 1024, 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_disk_space_insufficient() {
        // Request an absurdly large amount
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
    fn test_disk_space_status_eq() {
        assert_eq!(DiskSpaceStatus::Sufficient, DiskSpaceStatus::Sufficient);
        assert_eq!(DiskSpaceStatus::Low, DiskSpaceStatus::Low);
        assert_eq!(DiskSpaceStatus::Critical, DiskSpaceStatus::Critical);
        assert_ne!(DiskSpaceStatus::Sufficient, DiskSpaceStatus::Critical);
    }

    #[tokio::test]
    async fn test_disk_monitor_check() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024, 5);
        let status = monitor.check().await;
        // /tmp should have plenty of space
        assert_eq!(status, DiskSpaceStatus::Sufficient);
    }

    #[tokio::test]
    async fn test_disk_monitor_start_stop() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024 * 1024, 1);

        assert!(!monitor.is_running().await);

        monitor
            .start_monitoring(
                || async {},
                || async {},
            )
            .await;

        // Give it a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(monitor.is_running().await);

        monitor.stop_monitoring().await;
        // Give it a moment to stop
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!monitor.is_running().await);
    }

    #[tokio::test]
    async fn test_disk_monitor_double_start() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp"), 1024 * 1024, 1);

        monitor
            .start_monitoring(|| async {}, || async {})
            .await;
        // Second start should be a no-op
        monitor
            .start_monitoring(|| async {}, || async {})
            .await;

        assert!(monitor.is_running().await);
        monitor.stop_monitoring().await;
    }

    #[tokio::test]
    async fn test_disk_monitor_properties() {
        let monitor = DiskSpaceMonitor::new(PathBuf::from("/tmp/dl"), 500_000_000, 30);
        assert_eq!(monitor.monitor_path(), Path::new("/tmp/dl"));
        assert_eq!(monitor.safety_margin_bytes(), 500_000_000);
    }
}
