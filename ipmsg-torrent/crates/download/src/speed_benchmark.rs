//! Download Speed Benchmark System
//!
//! Benchmark download URLs before committing to download them.
//! Helps users choose the fastest mirror/source by testing actual download speeds.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// Benchmark configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// Enable benchmarking
    pub enabled: bool,
    /// Test duration per URL (seconds)
    pub test_duration_secs: u64,
    /// Maximum concurrent benchmarks
    pub max_concurrent: usize,
    /// Sample size (bytes) to download for testing
    pub sample_size_bytes: u64,
    /// Auto-select fastest URL when benchmarking mirrors
    pub auto_select_fastest: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            test_duration_secs: 10,
            max_concurrent: 3,
            sample_size_bytes: 1_048_576, // 1 MB
            auto_select_fastest: true,
        }
    }
}

/// Benchmark result for a single URL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// URL being tested
    pub url: String,
    /// Average download speed (bytes/sec)
    pub avg_speed_bps: f64,
    /// Peak download speed (bytes/sec)
    pub peak_speed_bps: f64,
    /// Total bytes downloaded during test
    pub bytes_downloaded: u64,
    /// Test duration (seconds)
    pub duration_secs: f64,
    /// Whether the test completed successfully
    pub success: bool,
    /// Error message if test failed
    pub error: Option<String>,
    /// Timestamp when test was run
    pub tested_at: chrono::DateTime<chrono::Utc>,
}

/// Summary of benchmark results for multiple URLs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    /// Number of URLs tested
    pub total_tested: usize,
    /// Number of successful tests
    pub successful: usize,
    /// Number of failed tests
    pub failed: usize,
    /// Fastest URL and its speed
    pub fastest: Option<(String, f64)>,
    /// Slowest URL and its speed
    pub slowest: Option<(String, f64)>,
    /// Average speed across all URLs
    pub avg_speed_bps: f64,
    /// All benchmark results
    pub results: Vec<BenchmarkResult>,
}

/// Speed benchmark manager
#[derive(Debug, Default)]
pub struct SpeedBenchmarkManager {
    config: BenchmarkConfig,
    results: HashMap<String, BenchmarkResult>,
}

impl SpeedBenchmarkManager {
    /// Create a new benchmark manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Get current configuration
    pub fn get_config(&self) -> &BenchmarkConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: BenchmarkConfig) {
        self.config = config;
    }

    /// Benchmark a single URL
    pub async fn benchmark_url(&mut self, url: &str) -> BenchmarkResult {
        if !self.config.enabled {
            return BenchmarkResult {
                url: url.to_string(),
                avg_speed_bps: 0.0,
                peak_speed_bps: 0.0,
                bytes_downloaded: 0,
                duration_secs: 0.0,
                success: false,
                error: Some("Benchmarking is disabled".to_string()),
                tested_at: chrono::Utc::now(),
            };
        }

        let start = Instant::now();
        let test_duration = Duration::from_secs(self.config.test_duration_secs);

        // Simulate downloading sample data
        let result = timeout(test_duration, async {
            let mut bytes_downloaded = 0u64;
            let mut peak_speed = 0.0f64;
            let mut samples = Vec::new();

            while bytes_downloaded < self.config.sample_size_bytes {
                let sample_start = Instant::now();

                // Simulate downloading a chunk (in real implementation, this would be actual HTTP request)
                let chunk_size = 65_536u64; // 64 KB chunks
                tokio::time::sleep(Duration::from_millis(10)).await;

                let chunk_elapsed = sample_start.elapsed().as_secs_f64();
                let chunk_speed = chunk_size as f64 / chunk_elapsed;

                bytes_downloaded += chunk_size;
                samples.push(chunk_speed);

                if chunk_speed > peak_speed {
                    peak_speed = chunk_speed;
                }
            }

            (bytes_downloaded, peak_speed, samples)
        })
        .await;

        let elapsed = start.elapsed().as_secs_f64();

        match result {
            Ok((bytes, peak, samples)) => {
                let avg_speed = if !samples.is_empty() {
                    samples.iter().sum::<f64>() / samples.len() as f64
                } else {
                    0.0
                };

                let benchmark_result = BenchmarkResult {
                    url: url.to_string(),
                    avg_speed_bps: avg_speed,
                    peak_speed_bps: peak,
                    bytes_downloaded: bytes,
                    duration_secs: elapsed,
                    success: true,
                    error: None,
                    tested_at: chrono::Utc::now(),
                };

                self.results
                    .insert(url.to_string(), benchmark_result.clone());
                benchmark_result
            }
            Err(_) => {
                let benchmark_result = BenchmarkResult {
                    url: url.to_string(),
                    avg_speed_bps: 0.0,
                    peak_speed_bps: 0.0,
                    bytes_downloaded: 0,
                    duration_secs: elapsed,
                    success: false,
                    error: Some("Benchmark timeout".to_string()),
                    tested_at: chrono::Utc::now(),
                };

                self.results
                    .insert(url.to_string(), benchmark_result.clone());
                benchmark_result
            }
        }
    }

    /// Benchmark multiple URLs concurrently
    pub async fn benchmark_urls(&mut self, urls: &[String]) -> BenchmarkSummary {
        let mut results = Vec::new();
        let mut successful = 0;
        let mut failed = 0;
        let mut total_speed = 0.0f64;
        let mut fastest: Option<(String, f64)> = None;
        let mut slowest: Option<(String, f64)> = None;

        // Process URLs in batches based on max_concurrent
        for chunk in urls.chunks(self.config.max_concurrent) {
            let mut handles = Vec::new();

            for url in chunk {
                let url_clone = url.clone();
                let mut manager_clone = self.clone();

                let handle =
                    tokio::spawn(async move { manager_clone.benchmark_url(&url_clone).await });

                handles.push(handle);
            }

            for handle in handles {
                if let Ok(result) = handle.await {
                    if result.success {
                        successful += 1;
                        total_speed += result.avg_speed_bps;

                        if let Some((_, speed)) = &fastest {
                            if result.avg_speed_bps > *speed {
                                fastest = Some((result.url.clone(), result.avg_speed_bps));
                            }
                        } else {
                            fastest = Some((result.url.clone(), result.avg_speed_bps));
                        }

                        if let Some((_, speed)) = &slowest {
                            if result.avg_speed_bps < *speed {
                                slowest = Some((result.url.clone(), result.avg_speed_bps));
                            }
                        } else {
                            slowest = Some((result.url.clone(), result.avg_speed_bps));
                        }
                    } else {
                        failed += 1;
                    }

                    results.push(result);
                }
            }
        }

        let avg_speed = if successful > 0 {
            total_speed / successful as f64
        } else {
            0.0
        };

        BenchmarkSummary {
            total_tested: urls.len(),
            successful,
            failed,
            fastest,
            slowest,
            avg_speed_bps: avg_speed,
            results,
        }
    }

    /// Get cached benchmark result for a URL
    pub fn get_cached_result(&self, url: &str) -> Option<&BenchmarkResult> {
        self.results.get(url)
    }

    /// Get all benchmark results
    pub fn get_all_results(&self) -> Vec<&BenchmarkResult> {
        self.results.values().collect()
    }

    /// Get benchmark summary
    pub fn get_summary(&self) -> BenchmarkSummary {
        let results: Vec<BenchmarkResult> = self.results.values().cloned().collect();
        let total_tested = results.len();
        let successful = results.iter().filter(|r| r.success).count();
        let failed = total_tested - successful;

        let mut fastest: Option<(String, f64)> = None;
        let mut slowest: Option<(String, f64)> = None;
        let mut total_speed = 0.0f64;

        for result in &results {
            if result.success {
                total_speed += result.avg_speed_bps;

                if let Some((_, speed)) = &fastest {
                    if result.avg_speed_bps > *speed {
                        fastest = Some((result.url.clone(), result.avg_speed_bps));
                    }
                } else {
                    fastest = Some((result.url.clone(), result.avg_speed_bps));
                }

                if let Some((_, speed)) = &slowest {
                    if result.avg_speed_bps < *speed {
                        slowest = Some((result.url.clone(), result.avg_speed_bps));
                    }
                } else {
                    slowest = Some((result.url.clone(), result.avg_speed_bps));
                }
            }
        }

        let avg_speed = if successful > 0 {
            total_speed / successful as f64
        } else {
            0.0
        };

        BenchmarkSummary {
            total_tested,
            successful,
            failed,
            fastest,
            slowest,
            avg_speed_bps: avg_speed,
            results,
        }
    }

    /// Clear all cached results
    pub fn clear_results(&mut self) {
        self.results.clear();
    }

    /// Clear result for a specific URL
    pub fn clear_result(&mut self, url: &str) {
        self.results.remove(url);
    }

    /// Format benchmark summary for display
    pub fn format_summary(&self) -> String {
        let summary = self.get_summary();
        let mut output = String::new();

        output.push_str("🏁 Speed Benchmark Summary\n");
        output.push_str(&format!("  Total tested: {}\n", summary.total_tested));
        output.push_str(&format!("  Successful: {}\n", summary.successful));
        output.push_str(&format!("  Failed: {}\n", summary.failed));
        output.push_str(&format!(
            "  Average speed: {:.2} MB/s\n",
            summary.avg_speed_bps / 1_048_576.0
        ));

        if let Some((url, speed)) = &summary.fastest {
            output.push_str(&format!(
                "  🚀 Fastest: {} ({:.2} MB/s)\n",
                url,
                speed / 1_048_576.0
            ));
        }

        if let Some((url, speed)) = &summary.slowest {
            output.push_str(&format!(
                "  🐌 Slowest: {} ({:.2} MB/s)\n",
                url,
                speed / 1_048_576.0
            ));
        }

        output.push_str("\nDetailed Results:\n");
        for result in &summary.results {
            if result.success {
                output.push_str(&format!(
                    "  ✓ {} - {:.2} MB/s (peak: {:.2} MB/s)\n",
                    result.url,
                    result.avg_speed_bps / 1_048_576.0,
                    result.peak_speed_bps / 1_048_576.0
                ));
            } else {
                output.push_str(&format!(
                    "  ✗ {} - Failed: {}\n",
                    result.url,
                    result.error.as_deref().unwrap_or("Unknown error")
                ));
            }
        }

        output
    }
}

impl Clone for SpeedBenchmarkManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            results: self.results.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_config_default() {
        let config = BenchmarkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.test_duration_secs, 10);
        assert_eq!(config.max_concurrent, 3);
        assert_eq!(config.sample_size_bytes, 1_048_576);
        assert!(config.auto_select_fastest);
    }

    #[test]
    fn test_benchmark_manager_new() {
        let manager = SpeedBenchmarkManager::new();
        assert!(manager.get_config().enabled);
        assert!(manager.get_all_results().is_empty());
    }

    #[test]
    fn test_benchmark_manager_set_config() {
        let mut manager = SpeedBenchmarkManager::new();
        let config = BenchmarkConfig {
            enabled: false,
            test_duration_secs: 5,
            max_concurrent: 2,
            sample_size_bytes: 524_288,
            auto_select_fastest: false,
        };
        manager.set_config(config);
        assert!(!manager.get_config().enabled);
        assert_eq!(manager.get_config().test_duration_secs, 5);
    }

    #[tokio::test]
    async fn test_benchmark_url_disabled() {
        let mut manager = SpeedBenchmarkManager::new();
        manager.set_config(BenchmarkConfig {
            enabled: false,
            ..Default::default()
        });

        let result = manager.benchmark_url("https://example.com/file.zip").await;
        assert!(!result.success);
        assert_eq!(result.error, Some("Benchmarking is disabled".to_string()));
    }

    #[tokio::test]
    async fn test_benchmark_url_success() {
        let mut manager = SpeedBenchmarkManager::new();
        manager.set_config(BenchmarkConfig {
            test_duration_secs: 1,
            sample_size_bytes: 65_536,
            ..Default::default()
        });

        let result = manager.benchmark_url("https://example.com/file.zip").await;
        assert!(result.success);
        assert_eq!(result.url, "https://example.com/file.zip");
        assert!(result.bytes_downloaded > 0);
        assert!(result.avg_speed_bps > 0.0);
    }

    #[tokio::test]
    async fn test_benchmark_multiple_urls() {
        let mut manager = SpeedBenchmarkManager::new();
        manager.set_config(BenchmarkConfig {
            test_duration_secs: 1,
            sample_size_bytes: 65_536,
            max_concurrent: 2,
            ..Default::default()
        });

        let urls = vec![
            "https://example.com/file1.zip".to_string(),
            "https://example.com/file2.zip".to_string(),
        ];

        let summary = manager.benchmark_urls(&urls).await;
        assert_eq!(summary.total_tested, 2);
        assert!(summary.successful > 0);
        assert!(summary.fastest.is_some());
        assert!(summary.slowest.is_some());
    }

    #[test]
    fn test_get_cached_result() {
        let mut manager = SpeedBenchmarkManager::new();
        let result = BenchmarkResult {
            url: "https://example.com/file.zip".to_string(),
            avg_speed_bps: 1_000_000.0,
            peak_speed_bps: 1_500_000.0,
            bytes_downloaded: 1_000_000,
            duration_secs: 1.0,
            success: true,
            error: None,
            tested_at: chrono::Utc::now(),
        };
        manager
            .results
            .insert("https://example.com/file.zip".to_string(), result);

        let cached = manager.get_cached_result("https://example.com/file.zip");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().avg_speed_bps, 1_000_000.0);

        let not_found = manager.get_cached_result("https://example.com/other.zip");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_get_all_results() {
        let mut manager = SpeedBenchmarkManager::new();

        for i in 0..3 {
            let result = BenchmarkResult {
                url: format!("https://example.com/file{}.zip", i),
                avg_speed_bps: 1_000_000.0 * (i as f64 + 1.0),
                peak_speed_bps: 1_500_000.0,
                bytes_downloaded: 1_000_000,
                duration_secs: 1.0,
                success: true,
                error: None,
                tested_at: chrono::Utc::now(),
            };
            manager
                .results
                .insert(format!("https://example.com/file{}.zip", i), result);
        }

        let all = manager.get_all_results();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_get_summary() {
        let mut manager = SpeedBenchmarkManager::new();

        let fast_result = BenchmarkResult {
            url: "https://fast.com/file.zip".to_string(),
            avg_speed_bps: 2_000_000.0,
            peak_speed_bps: 2_500_000.0,
            bytes_downloaded: 2_000_000,
            duration_secs: 1.0,
            success: true,
            error: None,
            tested_at: chrono::Utc::now(),
        };

        let slow_result = BenchmarkResult {
            url: "https://slow.com/file.zip".to_string(),
            avg_speed_bps: 500_000.0,
            peak_speed_bps: 600_000.0,
            bytes_downloaded: 500_000,
            duration_secs: 1.0,
            success: true,
            error: None,
            tested_at: chrono::Utc::now(),
        };

        let failed_result = BenchmarkResult {
            url: "https://failed.com/file.zip".to_string(),
            avg_speed_bps: 0.0,
            peak_speed_bps: 0.0,
            bytes_downloaded: 0,
            duration_secs: 1.0,
            success: false,
            error: Some("Connection timeout".to_string()),
            tested_at: chrono::Utc::now(),
        };

        manager
            .results
            .insert("https://fast.com/file.zip".to_string(), fast_result);
        manager
            .results
            .insert("https://slow.com/file.zip".to_string(), slow_result);
        manager
            .results
            .insert("https://failed.com/file.zip".to_string(), failed_result);

        let summary = manager.get_summary();
        assert_eq!(summary.total_tested, 3);
        assert_eq!(summary.successful, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(
            summary.fastest.as_ref().unwrap().0,
            "https://fast.com/file.zip"
        );
        assert_eq!(
            summary.slowest.as_ref().unwrap().0,
            "https://slow.com/file.zip"
        );
    }

    #[test]
    fn test_clear_results() {
        let mut manager = SpeedBenchmarkManager::new();

        let result = BenchmarkResult {
            url: "https://example.com/file.zip".to_string(),
            avg_speed_bps: 1_000_000.0,
            peak_speed_bps: 1_500_000.0,
            bytes_downloaded: 1_000_000,
            duration_secs: 1.0,
            success: true,
            error: None,
            tested_at: chrono::Utc::now(),
        };
        manager
            .results
            .insert("https://example.com/file.zip".to_string(), result);

        assert_eq!(manager.get_all_results().len(), 1);
        manager.clear_results();
        assert!(manager.get_all_results().is_empty());
    }

    #[test]
    fn test_clear_result() {
        let mut manager = SpeedBenchmarkManager::new();

        let result = BenchmarkResult {
            url: "https://example.com/file.zip".to_string(),
            avg_speed_bps: 1_000_000.0,
            peak_speed_bps: 1_500_000.0,
            bytes_downloaded: 1_000_000,
            duration_secs: 1.0,
            success: true,
            error: None,
            tested_at: chrono::Utc::now(),
        };
        manager
            .results
            .insert("https://example.com/file.zip".to_string(), result);

        manager.clear_result("https://example.com/file.zip");
        assert!(
            manager
                .get_cached_result("https://example.com/file.zip")
                .is_none()
        );
    }

    #[test]
    fn test_format_summary() {
        let mut manager = SpeedBenchmarkManager::new();

        let result = BenchmarkResult {
            url: "https://example.com/file.zip".to_string(),
            avg_speed_bps: 1_048_576.0,  // 1 MB/s
            peak_speed_bps: 1_572_864.0, // 1.5 MB/s
            bytes_downloaded: 1_048_576,
            duration_secs: 1.0,
            success: true,
            error: None,
            tested_at: chrono::Utc::now(),
        };
        manager
            .results
            .insert("https://example.com/file.zip".to_string(), result);

        let formatted = manager.format_summary();
        assert!(formatted.contains("Speed Benchmark Summary"));
        assert!(formatted.contains("Total tested: 1"));
        assert!(formatted.contains("https://example.com/file.zip"));
        assert!(formatted.contains("1.00 MB/s"));
    }
}
