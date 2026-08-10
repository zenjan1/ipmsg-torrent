//! Download Source Speed Benchmark (Phase 120)
//!
//! Before starting a download, benchmark all available source URLs by sending
//! small HTTP Range requests to measure actual download speed. Automatically
//! select the fastest source for the download.
//!
//! Features:
//! - Concurrent benchmarking of multiple sources
//! - HTTP Range requests for minimal data usage
//! - Configurable test size and timeout
//! - Speed ranking with latency, throughput, and success rate
//! - Persistent benchmark results cache for reuse
//! - Auto-selection of fastest source

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs;

/// Error type for source benchmark operations.
#[derive(Debug, thiserror::Error)]
pub enum SourceBenchmarkError {
    #[error("no sources to benchmark")]
    NoSources,
    #[error("all sources failed benchmark")]
    AllFailed,
    #[error("invalid source URL: {0}")]
    InvalidUrl(String),
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Configuration for source benchmarking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// Whether source benchmarking is enabled.
    pub enabled: bool,
    /// Number of bytes to download for each benchmark test (default: 64KB).
    pub test_size_bytes: u64,
    /// Maximum time to wait for each source benchmark (seconds, default: 10).
    pub timeout_secs: u64,
    /// Maximum number of sources to benchmark concurrently (default: 5).
    pub max_concurrent: usize,
    /// Minimum number of samples before caching results (default: 1).
    pub min_samples_for_cache: u32,
    /// How long cached benchmark results remain valid (hours, default: 24).
    pub cache_ttl_hours: u64,
    /// Maximum number of cached domain results (default: 200).
    pub max_cache_entries: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            test_size_bytes: 64 * 1024, // 64 KB
            timeout_secs: 10,
            max_concurrent: 5,
            min_samples_for_cache: 1,
            cache_ttl_hours: 24,
            max_cache_entries: 200,
        }
    }
}

/// Result of benchmarking a single source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBenchmarkResult {
    /// The source URL that was tested.
    pub url: String,
    /// Whether the benchmark succeeded.
    pub success: bool,
    /// Measured download speed in bytes/sec (0 if failed).
    pub speed_bps: f64,
    /// Connection latency in milliseconds (time to first byte).
    pub latency_ms: f64,
    /// HTTP status code received (0 if connection failed).
    pub http_status: u16,
    /// Total bytes actually downloaded during test.
    pub bytes_downloaded: u64,
    /// Time taken for the benchmark test.
    pub duration_ms: f64,
    /// Error message if the benchmark failed.
    pub error: Option<String>,
    /// When this benchmark was performed.
    pub tested_at: DateTime<Utc>,
}

impl SourceBenchmarkResult {
    /// Create a failed benchmark result.
    fn failed(url: String, error: String) -> Self {
        Self {
            url,
            success: false,
            speed_bps: 0.0,
            latency_ms: 0.0,
            http_status: 0,
            bytes_downloaded: 0,
            duration_ms: 0.0,
            error: Some(error),
            tested_at: Utc::now(),
        }
    }
}

/// Summary of a benchmark run across multiple sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    /// Total number of sources tested.
    pub total_sources: usize,
    /// Number of sources that succeeded.
    pub successful: usize,
    /// Number of sources that failed.
    pub failed: usize,
    /// Fastest source URL.
    pub fastest_url: Option<String>,
    /// Fastest source speed in bytes/sec.
    pub fastest_speed_bps: f64,
    /// Slowest successful source speed in bytes/sec.
    pub slowest_speed_bps: f64,
    /// Average speed across all successful sources.
    pub avg_speed_bps: f64,
    /// All benchmark results sorted by speed (fastest first).
    pub results: Vec<SourceBenchmarkResult>,
    /// Time taken for the entire benchmark run (ms).
    pub total_duration_ms: f64,
}

impl BenchmarkSummary {
    /// Get a human-readable summary.
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str(&format!(
            "🏎️ Source Benchmark Report\n\
             ═══════════════════════════\n\
             Sources tested: {} ({} ✅, {} ❌)\n\
             Total time: {:.0}ms\n",
            self.total_sources, self.successful, self.failed, self.total_duration_ms,
        ));

        if let Some(ref fastest) = self.fastest_url {
            report.push_str(&format!(
                "\n🏆 Fastest: {} ({})\n",
                fastest,
                format_speed_bps(self.fastest_speed_bps),
            ));
        }

        if self.avg_speed_bps > 0.0 {
            report.push_str(&format!(
                "📊 Average: {}\n",
                format_speed_bps(self.avg_speed_bps),
            ));
        }

        if !self.results.is_empty() {
            report.push_str("\nRankings:\n");
            for (i, result) in self.results.iter().enumerate() {
                if result.success {
                    report.push_str(&format!(
                        "  {}. {} — {} (latency: {:.0}ms)\n",
                        i + 1,
                        truncate_url(&result.url, 50),
                        format_speed_bps(result.speed_bps),
                        result.latency_ms,
                    ));
                } else {
                    report.push_str(&format!(
                        "  {}. {} — ❌ {}\n",
                        i + 1,
                        truncate_url(&result.url, 50),
                        result.error.as_deref().unwrap_or("unknown error"),
                    ));
                }
            }
        }

        report
    }
}

/// Cached benchmark result for a domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedDomainBenchmark {
    /// Domain name (e.g., "example.com").
    pub domain: String,
    /// Average speed measured for this domain (bytes/sec).
    pub avg_speed_bps: f64,
    /// Number of times this domain has been benchmarked.
    pub sample_count: u32,
    /// Last time this domain was benchmarked.
    pub last_tested_at: DateTime<Utc>,
    /// Whether this domain is known to be fast (> 1MB/s).
    pub is_fast: bool,
    /// Whether this domain is known to be slow (< 100KB/s).
    pub is_slow: bool,
}

/// Source benchmark results cache.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BenchmarkCache {
    /// Cached results by domain.
    pub domains: HashMap<String, CachedDomainBenchmark>,
}

/// Manager for source benchmarking.
pub struct SourceBenchmarkManager {
    config: BenchmarkConfig,
    cache: BenchmarkCache,
    data_dir: PathBuf,
}

impl SourceBenchmarkManager {
    /// Create a new source benchmark manager.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            config: BenchmarkConfig::default(),
            cache: BenchmarkCache::default(),
            data_dir,
        }
    }

    /// Create with a specific configuration.
    pub fn with_config(config: BenchmarkConfig, data_dir: PathBuf) -> Self {
        Self {
            config,
            cache: BenchmarkCache::default(),
            data_dir,
        }
    }

    /// Get current configuration.
    pub fn config(&self) -> &BenchmarkConfig {
        &self.config
    }

    /// Update configuration.
    pub fn set_config(&mut self, config: BenchmarkConfig) {
        self.config = config;
    }

    /// Benchmark a list of source URLs and return results sorted by speed.
    pub async fn benchmark_sources(
        &self,
        urls: &[String],
    ) -> Result<BenchmarkSummary, SourceBenchmarkError> {
        if urls.is_empty() {
            return Err(SourceBenchmarkError::NoSources);
        }

        let start = Instant::now();
        let timeout = Duration::from_secs(self.config.timeout_secs);
        let test_size = self.config.test_size_bytes;
        let max_concurrent = self.config.max_concurrent;

        // Benchmark all sources with concurrency limit
        let mut results = Vec::with_capacity(urls.len());
        for chunk in urls.chunks(max_concurrent) {
            let mut handles = Vec::new();
            for url in chunk {
                let url = url.clone();
                let timeout = timeout;
                let test_size = test_size;
                handles.push(tokio::spawn(async move {
                    benchmark_single_source(&url, test_size, timeout).await
                }));
            }

            for handle in handles {
                match handle.await {
                    Ok(result) => results.push(result),
                    Err(e) => {
                        results.push(SourceBenchmarkResult::failed(
                            String::new(),
                            format!("task join error: {}", e),
                        ));
                    }
                }
            }
        }

        // Sort results: successful by speed (fastest first), then failed
        results.sort_by(|a, b| match (a.success, b.success) {
            (true, true) => b
                .speed_bps
                .partial_cmp(&a.speed_bps)
                .unwrap_or(std::cmp::Ordering::Equal),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => std::cmp::Ordering::Equal,
        });

        let successful = results.iter().filter(|r| r.success).count();
        let failed = results.len() - successful;

        let speeds: Vec<f64> = results
            .iter()
            .filter(|r| r.success)
            .map(|r| r.speed_bps)
            .collect();
        let fastest_speed = speeds.first().copied().unwrap_or(0.0);
        let slowest_speed = speeds.last().copied().unwrap_or(0.0);
        let avg_speed = if speeds.is_empty() {
            0.0
        } else {
            speeds.iter().sum::<f64>() / speeds.len() as f64
        };
        let fastest_url = results.iter().find(|r| r.success).map(|r| r.url.clone());

        let total_duration = start.elapsed().as_secs_f64() * 1000.0;

        Ok(BenchmarkSummary {
            total_sources: urls.len(),
            successful,
            failed,
            fastest_url,
            fastest_speed_bps: fastest_speed,
            slowest_speed_bps: slowest_speed,
            avg_speed_bps: avg_speed,
            results,
            total_duration_ms: total_duration,
        })
    }

    /// Select the best source URL from a list, using benchmark results and cache.
    pub async fn select_best_source(
        &mut self,
        urls: &[String],
    ) -> Result<String, SourceBenchmarkError> {
        if urls.is_empty() {
            return Err(SourceBenchmarkError::NoSources);
        }

        if urls.len() == 1 {
            return Ok(urls[0].clone());
        }

        // Check cache first for known domains
        let mut cached_scores: HashMap<String, f64> = HashMap::new();
        self.refresh_cache();
        for url in urls {
            if let Some(domain) = extract_domain(url) {
                if let Some(cached) = self.cache.domains.get(&domain) {
                    cached_scores.insert(url.clone(), cached.avg_speed_bps);
                }
            }
        }

        // If all URLs have cache entries, use cached scores to pick the best
        if cached_scores.len() == urls.len() {
            let best = cached_scores
                .iter()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(url, _)| url.clone());
            if let Some(best) = best {
                return Ok(best);
            }
        }

        // Otherwise, run actual benchmarks
        match self.benchmark_sources(urls).await {
            Ok(summary) => {
                // Update cache with benchmark results
                self.update_cache_from_summary(&summary);

                if let Some(fastest) = summary.fastest_url {
                    Ok(fastest)
                } else {
                    // All failed, return first URL as fallback
                    Err(SourceBenchmarkError::AllFailed)
                }
            }
            Err(_) => {
                // Benchmark failed, fall back to first URL
                Ok(urls[0].clone())
            }
        }
    }

    /// Update the domain cache from benchmark summary results.
    fn update_cache_from_summary(&mut self, summary: &BenchmarkSummary) {
        for result in &summary.results {
            if !result.success {
                continue;
            }
            if let Some(domain) = extract_domain(&result.url) {
                let entry = self.cache.domains.entry(domain.clone()).or_insert_with(|| {
                    CachedDomainBenchmark {
                        domain: domain.clone(),
                        avg_speed_bps: 0.0,
                        sample_count: 0,
                        last_tested_at: Utc::now(),
                        is_fast: false,
                        is_slow: false,
                    }
                });

                // Exponential moving average
                let alpha = 0.3;
                if entry.sample_count == 0 {
                    entry.avg_speed_bps = result.speed_bps;
                } else {
                    entry.avg_speed_bps =
                        alpha * result.speed_bps + (1.0 - alpha) * entry.avg_speed_bps;
                }
                entry.sample_count += 1;
                entry.last_tested_at = Utc::now();
                entry.is_fast = entry.avg_speed_bps > 1_000_000.0; // > 1MB/s
                entry.is_slow = entry.avg_speed_bps < 100_000.0; // < 100KB/s
            }
        }

        // Evict old entries if cache is too large
        while self.cache.domains.len() > self.config.max_cache_entries {
            let oldest = self
                .cache
                .domains
                .iter()
                .min_by_key(|(_, v)| v.last_tested_at)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                self.cache.domains.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Remove expired entries from the cache.
    fn refresh_cache(&mut self) {
        let ttl = Duration::from_secs(self.config.cache_ttl_hours * 3600);
        let now = Utc::now();
        self.cache.domains.retain(|_, v| {
            now.signed_duration_since(v.last_tested_at)
                .to_std()
                .unwrap_or_default()
                < ttl
        });
    }

    /// Get the current benchmark cache.
    pub fn cache(&self) -> &BenchmarkCache {
        &self.cache
    }

    /// Get a cached domain benchmark result.
    pub fn get_cached_domain(&self, domain: &str) -> Option<&CachedDomainBenchmark> {
        self.cache.domains.get(domain)
    }

    /// Clear all cached benchmark results.
    pub fn clear_cache(&mut self) {
        self.cache.domains.clear();
    }

    /// Get a summary of the cache.
    pub fn cache_summary(&self) -> BenchmarkCacheSummary {
        let total = self.cache.domains.len();
        let fast = self.cache.domains.values().filter(|v| v.is_fast).count();
        let slow = self.cache.domains.values().filter(|v| v.is_slow).count();
        BenchmarkCacheSummary {
            total_domains: total,
            fast_domains: fast,
            slow_domains: slow,
        }
    }

    /// Save benchmark cache to disk.
    pub async fn save_cache(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("source_benchmark_cache.json");
        let json = serde_json::to_string_pretty(&self.cache)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, &json).await?;
        fs::rename(&tmp, &path).await?;
        Ok(())
    }

    /// Load benchmark cache from disk.
    pub async fn load_cache(&mut self) -> std::io::Result<()> {
        let path = self.data_dir.join("source_benchmark_cache.json");
        if !path.exists() {
            return Ok(());
        }
        let data = fs::read_to_string(&path).await?;
        match serde_json::from_str::<BenchmarkCache>(&data) {
            Ok(cache) => self.cache = cache,
            Err(_) => {
                // Corrupted cache file, start fresh
                self.cache = BenchmarkCache::default();
            }
        }
        Ok(())
    }
}

/// Summary of the benchmark cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkCacheSummary {
    /// Total number of cached domains.
    pub total_domains: usize,
    /// Number of fast domains (> 1MB/s).
    pub fast_domains: usize,
    /// Number of slow domains (< 100KB/s).
    pub slow_domains: usize,
}

/// Benchmark a single source URL by downloading a small range of bytes.
async fn benchmark_single_source(
    url: &str,
    test_size: u64,
    timeout: Duration,
) -> SourceBenchmarkResult {
    let start = Instant::now();

    // Validate URL
    if url.is_empty() || (!url.starts_with("http://") && !url.starts_with("https://")) {
        return SourceBenchmarkResult::failed(
            url.to_string(),
            "unsupported protocol (only HTTP/HTTPS)".to_string(),
        );
    }

    // Create HTTP client with timeout
    let client = match reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return SourceBenchmarkResult::failed(url.to_string(), format!("client error: {}", e));
        }
    };

    // Send HEAD request first to check availability and get content length
    let head_result = client.head(url).send().await;
    let latency_start = Instant::now();
    let content_length = match head_result {
        Ok(resp) => {
            let latency_ms = latency_start.elapsed().as_secs_f64() * 1000.0;
            let status = resp.status().as_u16();
            if !resp.status().is_success() && status != 206 {
                return SourceBenchmarkResult {
                    url: url.to_string(),
                    success: false,
                    speed_bps: 0.0,
                    latency_ms,
                    http_status: status,
                    bytes_downloaded: 0,
                    duration_ms: start.elapsed().as_secs_f64() * 1000.0,
                    error: Some(format!("HTTP {}", status)),
                    tested_at: Utc::now(),
                };
            }
            resp.headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        }
        Err(e) => {
            return SourceBenchmarkResult::failed(
                url.to_string(),
                format!("HEAD request failed: {}", e),
            );
        }
    };

    // Determine the range to request
    let end = if content_length > 0 {
        std::cmp::min(test_size, content_length) - 1
    } else {
        test_size - 1
    };

    // Send GET request with Range header
    let get_start = Instant::now();
    let get_result = client
        .get(url)
        .header("Range", format!("bytes=0-{}", end))
        .send()
        .await;

    match get_result {
        Ok(resp) => {
            let status = resp.status().as_u16();
            if !resp.status().is_success() && status != 206 {
                return SourceBenchmarkResult {
                    url: url.to_string(),
                    success: false,
                    speed_bps: 0.0,
                    latency_ms: get_start.elapsed().as_secs_f64() * 1000.0,
                    http_status: status,
                    bytes_downloaded: 0,
                    duration_ms: start.elapsed().as_secs_f64() * 1000.0,
                    error: Some(format!("GET HTTP {}", status)),
                    tested_at: Utc::now(),
                };
            }

            // Read response body
            match resp.bytes().await {
                Ok(bytes) => {
                    let duration = start.elapsed();
                    let duration_ms = duration.as_secs_f64() * 1000.0;
                    let bytes_len = bytes.len() as u64;
                    let speed_bps = if duration.as_secs_f64() > 0.0 {
                        bytes_len as f64 / duration.as_secs_f64()
                    } else {
                        0.0
                    };

                    SourceBenchmarkResult {
                        url: url.to_string(),
                        success: true,
                        speed_bps,
                        latency_ms: get_start.elapsed().as_secs_f64() * 1000.0,
                        http_status: status,
                        bytes_downloaded: bytes_len,
                        duration_ms,
                        error: None,
                        tested_at: Utc::now(),
                    }
                }
                Err(e) => SourceBenchmarkResult::failed(
                    url.to_string(),
                    format!("read body failed: {}", e),
                ),
            }
        }
        Err(e) => {
            SourceBenchmarkResult::failed(url.to_string(), format!("GET request failed: {}", e))
        }
    }
}

/// Extract domain from a URL string.
fn extract_domain(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
}

/// Format speed in bytes/sec to human-readable string.
fn format_speed_bps(bps: f64) -> String {
    if bps >= 1_000_000_000.0 {
        format!("{:.1} GB/s", bps / 1_000_000_000.0)
    } else if bps >= 1_000_000.0 {
        format!("{:.1} MB/s", bps / 1_000_000.0)
    } else if bps >= 1_000.0 {
        format!("{:.1} KB/s", bps / 1_000.0)
    } else {
        format!("{:.0} B/s", bps)
    }
}

/// Truncate a URL for display.
fn truncate_url(url: &str, max_len: usize) -> String {
    if url.len() <= max_len {
        url.to_string()
    } else {
        format!("{}...", &url[..max_len - 3])
    }
}

/// Save benchmark config to disk.
pub async fn save_benchmark_config(
    config: &BenchmarkConfig,
    data_dir: &Path,
) -> std::io::Result<()> {
    let path = data_dir.join("source_benchmark_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, &json).await?;
    fs::rename(&tmp, &path).await?;
    Ok(())
}

/// Load benchmark config from disk.
pub async fn load_benchmark_config(data_dir: &Path) -> Option<BenchmarkConfig> {
    let path = data_dir.join("source_benchmark_config.json");
    let data = fs::read_to_string(&path).await.ok()?;
    serde_json::from_str(&data).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BenchmarkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.test_size_bytes, 64 * 1024);
        assert_eq!(config.timeout_secs, 10);
        assert_eq!(config.max_concurrent, 5);
        assert_eq!(config.cache_ttl_hours, 24);
        assert_eq!(config.max_cache_entries, 200);
    }

    #[test]
    fn test_config_serialization() {
        let config = BenchmarkConfig {
            enabled: false,
            test_size_bytes: 128 * 1024,
            timeout_secs: 15,
            max_concurrent: 3,
            min_samples_for_cache: 2,
            cache_ttl_hours: 48,
            max_cache_entries: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BenchmarkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.test_size_bytes, 128 * 1024);
        assert_eq!(deserialized.timeout_secs, 15);
        assert_eq!(deserialized.max_concurrent, 3);
    }

    #[test]
    fn test_extract_domain() {
        assert_eq!(
            extract_domain("https://example.com/file.zip"),
            Some("example.com".to_string())
        );
        assert_eq!(
            extract_domain("http://sub.domain.org/path?q=1"),
            Some("sub.domain.org".to_string())
        );
        assert_eq!(extract_domain("not-a-url"), None);
        assert_eq!(extract_domain(""), None);
    }

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(500.0), "500 B/s");
        assert_eq!(format_speed_bps(1500.0), "1.5 KB/s");
        assert_eq!(format_speed_bps(1_500_000.0), "1.5 MB/s");
        assert_eq!(format_speed_bps(1_500_000_000.0), "1.5 GB/s");
        assert_eq!(format_speed_bps(0.0), "0 B/s");
    }

    #[test]
    fn test_truncate_url() {
        assert_eq!(truncate_url("short", 50), "short");
        let long_url = "https://example.com/very/long/path/to/file.zip?query=param&other=value";
        let truncated = truncate_url(long_url, 30);
        assert!(truncated.len() <= 30);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn test_benchmark_result_failed() {
        let result = SourceBenchmarkResult::failed(
            "https://example.com".to_string(),
            "connection refused".to_string(),
        );
        assert!(!result.success);
        assert_eq!(result.speed_bps, 0.0);
        assert_eq!(result.error, Some("connection refused".to_string()));
    }

    #[test]
    fn test_benchmark_summary_format() {
        let summary = BenchmarkSummary {
            total_sources: 3,
            successful: 2,
            failed: 1,
            fastest_url: Some("https://fast.example.com".to_string()),
            fastest_speed_bps: 5_000_000.0,
            slowest_speed_bps: 500_000.0,
            avg_speed_bps: 2_750_000.0,
            results: vec![
                SourceBenchmarkResult {
                    url: "https://fast.example.com".to_string(),
                    success: true,
                    speed_bps: 5_000_000.0,
                    latency_ms: 50.0,
                    http_status: 206,
                    bytes_downloaded: 65536,
                    duration_ms: 13.0,
                    error: None,
                    tested_at: Utc::now(),
                },
                SourceBenchmarkResult {
                    url: "https://slow.example.com".to_string(),
                    success: true,
                    speed_bps: 500_000.0,
                    latency_ms: 200.0,
                    http_status: 200,
                    bytes_downloaded: 65536,
                    duration_ms: 131.0,
                    error: None,
                    tested_at: Utc::now(),
                },
                SourceBenchmarkResult::failed(
                    "https://dead.example.com".to_string(),
                    "connection timeout".to_string(),
                ),
            ],
            total_duration_ms: 200.0,
        };

        let report = summary.format_report();
        assert!(report.contains("Source Benchmark Report"));
        assert!(report.contains("3"));
        assert!(report.contains("fast.example.com"));
        assert!(report.contains("5.0 MB/s"));
        assert!(report.contains("❌"));
    }

    #[test]
    fn test_cache_summary() {
        let mut cache = BenchmarkCache::default();
        cache.domains.insert(
            "fast.com".to_string(),
            CachedDomainBenchmark {
                domain: "fast.com".to_string(),
                avg_speed_bps: 5_000_000.0,
                sample_count: 3,
                last_tested_at: Utc::now(),
                is_fast: true,
                is_slow: false,
            },
        );
        cache.domains.insert(
            "slow.com".to_string(),
            CachedDomainBenchmark {
                domain: "slow.com".to_string(),
                avg_speed_bps: 50_000.0,
                sample_count: 1,
                last_tested_at: Utc::now(),
                is_fast: false,
                is_slow: true,
            },
        );
        cache.domains.insert(
            "medium.com".to_string(),
            CachedDomainBenchmark {
                domain: "medium.com".to_string(),
                avg_speed_bps: 500_000.0,
                sample_count: 2,
                last_tested_at: Utc::now(),
                is_fast: false,
                is_slow: false,
            },
        );

        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        mgr.cache = cache;
        let summary = mgr.cache_summary();
        assert_eq!(summary.total_domains, 3);
        assert_eq!(summary.fast_domains, 1);
        assert_eq!(summary.slow_domains, 1);
    }

    #[test]
    fn test_manager_config() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        assert!(mgr.config().enabled);

        let new_config = BenchmarkConfig {
            enabled: false,
            test_size_bytes: 32 * 1024,
            ..BenchmarkConfig::default()
        };
        mgr.set_config(new_config);
        assert!(!mgr.config().enabled);
        assert_eq!(mgr.config().test_size_bytes, 32 * 1024);
    }

    #[test]
    fn test_clear_cache() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        mgr.cache.domains.insert(
            "test.com".to_string(),
            CachedDomainBenchmark {
                domain: "test.com".to_string(),
                avg_speed_bps: 100_000.0,
                sample_count: 1,
                last_tested_at: Utc::now(),
                is_fast: false,
                is_slow: false,
            },
        );
        assert_eq!(mgr.cache().domains.len(), 1);
        mgr.clear_cache();
        assert_eq!(mgr.cache().domains.len(), 0);
    }

    #[test]
    fn test_get_cached_domain() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        mgr.cache.domains.insert(
            "example.com".to_string(),
            CachedDomainBenchmark {
                domain: "example.com".to_string(),
                avg_speed_bps: 1_000_000.0,
                sample_count: 5,
                last_tested_at: Utc::now(),
                is_fast: true,
                is_slow: false,
            },
        );

        assert!(mgr.get_cached_domain("example.com").is_some());
        assert!(mgr.get_cached_domain("unknown.com").is_none());
        let cached = mgr.get_cached_domain("example.com").unwrap();
        assert!(cached.is_fast);
        assert_eq!(cached.sample_count, 5);
    }

    #[test]
    fn test_update_cache_from_summary() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        let summary = BenchmarkSummary {
            total_sources: 2,
            successful: 1,
            failed: 1,
            fastest_url: Some("https://fast.com/file".to_string()),
            fastest_speed_bps: 2_000_000.0,
            slowest_speed_bps: 0.0,
            avg_speed_bps: 2_000_000.0,
            results: vec![
                SourceBenchmarkResult {
                    url: "https://fast.com/file".to_string(),
                    success: true,
                    speed_bps: 2_000_000.0,
                    latency_ms: 30.0,
                    http_status: 206,
                    bytes_downloaded: 65536,
                    duration_ms: 32.0,
                    error: None,
                    tested_at: Utc::now(),
                },
                SourceBenchmarkResult::failed(
                    "https://dead.com/file".to_string(),
                    "timeout".to_string(),
                ),
            ],
            total_duration_ms: 100.0,
        };

        mgr.update_cache_from_summary(&summary);
        assert_eq!(mgr.cache.domains.len(), 1);
        let cached = mgr.cache.domains.get("fast.com").unwrap();
        assert_eq!(cached.avg_speed_bps, 2_000_000.0);
        assert_eq!(cached.sample_count, 1);
        assert!(cached.is_fast);
    }

    #[test]
    fn test_cache_ema_update() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));

        // First sample
        let summary1 = BenchmarkSummary {
            total_sources: 1,
            successful: 1,
            failed: 0,
            fastest_url: Some("https://example.com/a".to_string()),
            fastest_speed_bps: 1_000_000.0,
            slowest_speed_bps: 1_000_000.0,
            avg_speed_bps: 1_000_000.0,
            results: vec![SourceBenchmarkResult {
                url: "https://example.com/a".to_string(),
                success: true,
                speed_bps: 1_000_000.0,
                latency_ms: 50.0,
                http_status: 200,
                bytes_downloaded: 65536,
                duration_ms: 65.0,
                error: None,
                tested_at: Utc::now(),
            }],
            total_duration_ms: 65.0,
        };
        mgr.update_cache_from_summary(&summary1);
        let first = mgr.cache.domains.get("example.com").unwrap().avg_speed_bps;
        assert_eq!(first, 1_000_000.0);

        // Second sample with different speed - EMA should blend
        let summary2 = BenchmarkSummary {
            total_sources: 1,
            successful: 1,
            failed: 0,
            fastest_url: Some("https://example.com/b".to_string()),
            fastest_speed_bps: 2_000_000.0,
            slowest_speed_bps: 2_000_000.0,
            avg_speed_bps: 2_000_000.0,
            results: vec![SourceBenchmarkResult {
                url: "https://example.com/b".to_string(),
                success: true,
                speed_bps: 2_000_000.0,
                latency_ms: 25.0,
                http_status: 200,
                bytes_downloaded: 65536,
                duration_ms: 32.0,
                error: None,
                tested_at: Utc::now(),
            }],
            total_duration_ms: 32.0,
        };
        mgr.update_cache_from_summary(&summary2);
        let second = mgr.cache.domains.get("example.com").unwrap().avg_speed_bps;
        // EMA: 0.3 * 2_000_000 + 0.7 * 1_000_000 = 1_300_000
        assert!((second - 1_300_000.0).abs() < 1.0);
        assert_eq!(
            mgr.cache.domains.get("example.com").unwrap().sample_count,
            2
        );
    }

    #[test]
    fn test_cache_eviction() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        mgr.config.max_cache_entries = 3;

        // Add 4 domains
        for i in 0..4 {
            let mut summary = BenchmarkSummary {
                total_sources: 1,
                successful: 1,
                failed: 0,
                fastest_url: None,
                fastest_speed_bps: 100_000.0,
                slowest_speed_bps: 100_000.0,
                avg_speed_bps: 100_000.0,
                results: vec![],
                total_duration_ms: 50.0,
            };
            summary.fastest_url = Some(format!("https://domain{}.com/file", i));
            summary.results.push(SourceBenchmarkResult {
                url: format!("https://domain{}.com/file", i),
                success: true,
                speed_bps: 100_000.0 * (i as f64 + 1.0),
                latency_ms: 50.0,
                http_status: 200,
                bytes_downloaded: 65536,
                duration_ms: 65.0,
                error: None,
                tested_at: Utc::now(),
            });
            mgr.update_cache_from_summary(&summary);
        }

        // Should have evicted the oldest, keeping only 3
        assert!(mgr.cache.domains.len() <= 3);
    }

    #[test]
    fn test_benchmark_non_http_url() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(benchmark_single_source(
            "magnet:?xt=urn:btih:abc123",
            65536,
            Duration::from_secs(5),
        ));
        assert!(!result.success);
        assert!(result.error.unwrap().contains("unsupported protocol"));
    }

    #[test]
    fn test_benchmark_empty_url() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(benchmark_single_source("", 65536, Duration::from_secs(5)));
        assert!(!result.success);
        assert!(result.error.unwrap().contains("unsupported protocol"));
    }

    #[tokio::test]
    async fn test_benchmark_no_sources_error() {
        let mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        let result = mgr.benchmark_sources(&[]).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            SourceBenchmarkError::NoSources => {}
            other => panic!("expected NoSources, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_select_best_single_source() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        let result = mgr
            .select_best_source(&["https://only-one.com/file.zip".to_string()])
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "https://only-one.com/file.zip");
    }

    #[tokio::test]
    async fn test_select_best_empty_sources_error() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        let result = mgr.select_best_source(&[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_and_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = BenchmarkConfig {
            enabled: false,
            test_size_bytes: 128 * 1024,
            timeout_secs: 20,
            max_concurrent: 10,
            min_samples_for_cache: 3,
            cache_ttl_hours: 12,
            max_cache_entries: 50,
        };

        save_benchmark_config(&config, dir.path()).await.unwrap();
        let loaded = load_benchmark_config(dir.path()).await.unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.test_size_bytes, 128 * 1024);
        assert_eq!(loaded.timeout_secs, 20);
        assert_eq!(loaded.max_concurrent, 10);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_benchmark_config(dir.path()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_save_and_load_cache() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = SourceBenchmarkManager::new(dir.path().to_path_buf());

        mgr.cache.domains.insert(
            "test.com".to_string(),
            CachedDomainBenchmark {
                domain: "test.com".to_string(),
                avg_speed_bps: 500_000.0,
                sample_count: 3,
                last_tested_at: Utc::now(),
                is_fast: false,
                is_slow: false,
            },
        );

        mgr.save_cache().await.unwrap();

        let mut mgr2 = SourceBenchmarkManager::new(dir.path().to_path_buf());
        mgr2.load_cache().await.unwrap();
        assert_eq!(mgr2.cache.domains.len(), 1);
        let cached = mgr2.cache.domains.get("test.com").unwrap();
        assert_eq!(cached.avg_speed_bps, 500_000.0);
        assert_eq!(cached.sample_count, 3);
    }

    #[tokio::test]
    async fn test_load_cache_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = SourceBenchmarkManager::new(dir.path().to_path_buf());
        mgr.load_cache().await.unwrap(); // Should not error
        assert_eq!(mgr.cache.domains.len(), 0);
    }

    #[tokio::test]
    async fn test_load_cache_corrupted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source_benchmark_cache.json");
        fs::write(&path, "not valid json{{{").await.unwrap();

        let mut mgr = SourceBenchmarkManager::new(dir.path().to_path_buf());
        mgr.load_cache().await.unwrap(); // Should not error, just start fresh
        assert_eq!(mgr.cache.domains.len(), 0);
    }

    #[test]
    fn test_benchmark_cache_serialization() {
        let mut cache = BenchmarkCache::default();
        cache.domains.insert(
            "example.com".to_string(),
            CachedDomainBenchmark {
                domain: "example.com".to_string(),
                avg_speed_bps: 1_500_000.0,
                sample_count: 10,
                last_tested_at: Utc::now(),
                is_fast: true,
                is_slow: false,
            },
        );

        let json = serde_json::to_string(&cache).unwrap();
        let deserialized: BenchmarkCache = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.domains.len(), 1);
        let entry = deserialized.domains.get("example.com").unwrap();
        assert_eq!(entry.avg_speed_bps, 1_500_000.0);
        assert!(entry.is_fast);
    }

    #[test]
    fn test_refresh_cache_removes_expired() {
        let mut mgr = SourceBenchmarkManager::new(PathBuf::from("/tmp/test_bm"));
        mgr.config.cache_ttl_hours = 1; // 1 hour TTL

        // Add an old entry (2 hours ago)
        let old_time = Utc::now() - chrono::Duration::hours(2);
        mgr.cache.domains.insert(
            "old.com".to_string(),
            CachedDomainBenchmark {
                domain: "old.com".to_string(),
                avg_speed_bps: 100_000.0,
                sample_count: 1,
                last_tested_at: old_time,
                is_fast: false,
                is_slow: false,
            },
        );

        // Add a fresh entry
        mgr.cache.domains.insert(
            "fresh.com".to_string(),
            CachedDomainBenchmark {
                domain: "fresh.com".to_string(),
                avg_speed_bps: 500_000.0,
                sample_count: 1,
                last_tested_at: Utc::now(),
                is_fast: false,
                is_slow: false,
            },
        );

        assert_eq!(mgr.cache.domains.len(), 2);
        mgr.refresh_cache();
        assert_eq!(mgr.cache.domains.len(), 1);
        assert!(mgr.cache.domains.contains_key("fresh.com"));
        assert!(!mgr.cache.domains.contains_key("old.com"));
    }

    #[test]
    fn test_benchmark_summary_all_failed() {
        let summary = BenchmarkSummary {
            total_sources: 2,
            successful: 0,
            failed: 2,
            fastest_url: None,
            fastest_speed_bps: 0.0,
            slowest_speed_bps: 0.0,
            avg_speed_bps: 0.0,
            results: vec![
                SourceBenchmarkResult::failed("https://a.com".to_string(), "timeout".to_string()),
                SourceBenchmarkResult::failed("https://b.com".to_string(), "404".to_string()),
            ],
            total_duration_ms: 20000.0,
        };

        let report = summary.format_report();
        assert!(report.contains("0 ✅"));
        assert!(report.contains("2 ❌"));
        assert!(!report.contains("🏆 Fastest"));
    }
}
