//! Token-bucket rate limiter for download speed control.
//!
//! Supports per-task and global speed limits. The limiter uses a
//! classic token-bucket algorithm: tokens accumulate at a fixed
//! rate up to a burst capacity, and each byte consumed removes
//! one token. When the bucket is empty the caller waits until
//! enough tokens are replenished.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

/// A single token-bucket limiter.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<BucketInner>>,
}

#[derive(Debug)]
struct BucketInner {
    /// Maximum bytes per second (0 = unlimited).
    bytes_per_sec: u64,
    /// Maximum burst size (tokens that can accumulate).
    burst: u64,
    /// Current available tokens (bytes).
    tokens: f64,
    /// Last time tokens were replenished.
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new limiter. `bytes_per_sec` of 0 means unlimited.
    pub fn new(bytes_per_sec: u64) -> Self {
        let burst = bytes_per_sec.max(1024); // at least 1 KB burst
        Self {
            inner: Arc::new(Mutex::new(BucketInner {
                bytes_per_sec,
                burst,
                tokens: burst as f64,
                last_refill: Instant::now(),
            })),
        }
    }

    /// Update the speed limit at runtime. 0 = unlimited.
    pub async fn set_speed(&self, bytes_per_sec: u64) {
        let mut inner = self.inner.lock().await;
        inner.bytes_per_sec = bytes_per_sec;
        inner.burst = bytes_per_sec.max(1024);
        // Reset tokens to new burst capacity
        inner.tokens = inner.burst as f64;
    }

    /// Current speed limit in bytes/sec (0 = unlimited).
    pub async fn speed_limit(&self) -> u64 {
        self.inner.lock().await.bytes_per_sec
    }

    /// Whether the limiter is currently enforcing a limit.
    pub async fn is_limited(&self) -> bool {
        self.inner.lock().await.bytes_per_sec > 0
    }

    /// Wait until `n` bytes are allowed, then consume them.
    ///
    /// If the limiter is unlimited (0 bps), returns immediately.
    pub async fn acquire(&self, n: u64) {
        loop {
            let wait = {
                let mut inner = self.inner.lock().await;
                if inner.bytes_per_sec == 0 {
                    return; // unlimited
                }
                Self::refill(&mut inner);
                if inner.tokens >= n as f64 {
                    inner.tokens -= n as f64;
                    return; // got tokens
                }
                // Calculate how long until enough tokens
                let deficit = n as f64 - inner.tokens;
                let wait_secs = deficit / inner.bytes_per_sec as f64;
                Duration::from_secs_f64(wait_secs)
            };
            // Sleep outside the lock
            tokio::time::sleep(wait.max(Duration::from_millis(1))).await;
        }
    }

    /// Try to consume up to `n` bytes without waiting.
    /// Returns the number of bytes actually allowed (may be 0).
    pub async fn try_acquire(&self, n: u64) -> u64 {
        let mut inner = self.inner.lock().await;
        if inner.bytes_per_sec == 0 {
            return n;
        }
        Self::refill(&mut inner);
        let available = inner.tokens.min(n as f64) as u64;
        inner.tokens -= available as f64;
        available
    }

    /// Refill tokens based on elapsed time. Must be called under lock.
    fn refill(inner: &mut BucketInner) {
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
        if elapsed <= 0.0 || inner.bytes_per_sec == 0 {
            return;
        }
        let new_tokens = elapsed * inner.bytes_per_sec as f64;
        inner.tokens = (inner.tokens + new_tokens).min(inner.burst as f64);
        inner.last_refill = now;
    }
}

/// Shared global + per-task rate limiter.
///
/// Downloads should call [`acquire`] before writing data to disk.
/// The effective limit is the minimum of the global and per-task
/// limits (both are checked).
#[derive(Debug, Clone)]
pub struct DownloadRateController {
    global: RateLimiter,
    per_task: RateLimiter,
}

impl DownloadRateController {
    pub fn new(global_limit: u64, task_limit: u64) -> Self {
        Self {
            global: RateLimiter::new(global_limit),
            per_task: RateLimiter::new(task_limit),
        }
    }

    /// Set the global speed limit (shared across all tasks).
    pub async fn set_global_limit(&self, bytes_per_sec: u64) {
        self.global.set_speed(bytes_per_sec).await;
    }

    /// Set the per-task speed limit.
    pub async fn set_task_limit(&self, bytes_per_sec: u64) {
        self.per_task.set_speed(bytes_per_sec).await;
    }

    /// Current global limit.
    pub async fn global_limit(&self) -> u64 {
        self.global.speed_limit().await
    }

    /// Current per-task limit.
    pub async fn task_limit(&self) -> u64 {
        self.per_task.speed_limit().await
    }

    /// Whether any limit is active.
    pub async fn is_limited(&self) -> bool {
        self.global.is_limited().await || self.per_task.is_limited().await
    }

    /// Wait until `n` bytes are allowed by both limiters.
    pub async fn acquire(&self, n: u64) {
        // Acquire from both limiters. The slower one determines actual wait.
        self.global.acquire(n).await;
        self.per_task.acquire(n).await;
    }

    /// Get the global limiter handle (for sharing across tasks).
    pub fn global(&self) -> &RateLimiter {
        &self.global
    }

    /// Get the per-task limiter handle.
    pub fn per_task(&self) -> &RateLimiter {
        &self.per_task
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unlimited_acquire() {
        let limiter = RateLimiter::new(0);
        // Should return immediately
        limiter.acquire(1_000_000).await;
        assert!(!limiter.is_limited().await);
    }

    #[tokio::test]
    async fn test_limited_acquire() {
        // 10 KB/s limit
        let limiter = RateLimiter::new(10_000);
        assert!(limiter.is_limited().await);
        assert_eq!(limiter.speed_limit().await, 10_000);

        // First burst should succeed immediately (burst = 10KB)
        let start = Instant::now();
        limiter.acquire(5_000).await;
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() < 50, "burst should be immediate");
    }

    #[tokio::test]
    async fn test_rate_limiting_enforced() {
        // 1000 bytes/sec, burst = 1000
        let limiter = RateLimiter::new(1000);

        // Consume the initial burst
        limiter.acquire(1000).await;

        // Next acquire should wait ~1 second for 1000 more bytes
        let start = Instant::now();
        limiter.acquire(1000).await;
        let elapsed = start.elapsed();

        // Should have waited at least 500ms (allowing for timing imprecision)
        assert!(
            elapsed.as_millis() >= 500,
            "expected wait, got {}ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_try_acquire() {
        let limiter = RateLimiter::new(1000);

        // First try should get what we asked for (within burst)
        let got = limiter.try_acquire(500).await;
        assert_eq!(got, 500);

        // Second try should get remaining burst (allowing for small time-based refill)
        let got = limiter.try_acquire(600).await;
        assert!(
            got >= 400 && got <= 524,
            "expected ~500 remaining, got {}",
            got
        );

        // Third try immediately should get very little (almost no time passed)
        let got = limiter.try_acquire(1000).await;
        assert!(got < 100, "should be nearly depleted, got {}", got);
    }

    #[tokio::test]
    async fn test_try_acquire_unlimited() {
        let limiter = RateLimiter::new(0);
        let got = limiter.try_acquire(999_999).await;
        assert_eq!(got, 999_999);
    }

    #[tokio::test]
    async fn test_set_speed() {
        let limiter = RateLimiter::new(1000);
        assert_eq!(limiter.speed_limit().await, 1000);

        limiter.set_speed(5000).await;
        assert_eq!(limiter.speed_limit().await, 5000);

        limiter.set_speed(0).await;
        assert!(!limiter.is_limited().await);
    }

    #[tokio::test]
    async fn test_download_rate_controller() {
        let ctrl = DownloadRateController::new(10_000, 5_000);
        assert!(ctrl.is_limited().await);
        assert_eq!(ctrl.global_limit().await, 10_000);
        assert_eq!(ctrl.task_limit().await, 5_000);

        ctrl.set_global_limit(0).await;
        assert!(ctrl.is_limited().await); // still limited by per-task

        ctrl.set_task_limit(0).await;
        assert!(!ctrl.is_limited().await);
    }

    #[tokio::test]
    async fn test_controller_acquire_unlimited() {
        let ctrl = DownloadRateController::new(0, 0);
        let start = Instant::now();
        ctrl.acquire(1_000_000).await;
        assert!(start.elapsed().as_millis() < 50);
    }

    #[tokio::test]
    async fn test_token_refill_over_time() {
        let limiter = RateLimiter::new(10_000); // 10KB/s

        // Drain the burst
        limiter.acquire(10_000).await;

        // Wait 100ms → should refill ~1000 bytes
        tokio::time::sleep(Duration::from_millis(110)).await;

        let got = limiter.try_acquire(1000).await;
        // Should have ~1000 tokens refilled (allow some timing margin)
        assert!(got >= 500, "expected ~1000 refilled, got {}", got);
    }

    #[tokio::test]
    async fn test_shared_limiter() {
        // Verify that cloning shares the same bucket
        let limiter = RateLimiter::new(1000);
        let clone = limiter.clone();

        // Consume burst via clone
        clone.acquire(1000).await;

        // Original should also be nearly depleted (allowing for small time-based refill)
        let got = limiter.try_acquire(1000).await;
        assert!(
            got < 100,
            "shared bucket should be nearly depleted, got {}",
            got
        );
    }
}
