//! Download speed test tool for measuring throughput to a URL before committing
//!
//! Performs a partial download (configurable sample size) to measure:
//! - Connection time (DNS + TCP + TLS)
//! - Time to first byte (TTFB)
//! - Download throughput
//! - Overall score/rating
//!
//! Results can help users decide whether to proceed with a full download.

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Configuration for speed test behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestConfig {
    /// Number of bytes to download for the test (default: 1MB)
    pub sample_size_bytes: u64,
    /// Timeout for the entire test in seconds (default: 30)
    pub timeout_secs: u64,
    /// Number of parallel connections to test (default: 1)
    pub parallel_connections: u32,
    /// Whether to include DNS resolution time (default: true)
    pub include_dns_time: bool,
    /// Minimum speed to consider "good" in bytes/sec (default: 100KB/s)
    pub good_speed_threshold_bps: u64,
    /// Minimum speed to consider "acceptable" in bytes/sec (default: 50KB/s)
    pub acceptable_speed_threshold_bps: u64,
}

impl Default for SpeedTestConfig {
    fn default() -> Self {
        Self {
            sample_size_bytes: 1_048_576, // 1 MB
            timeout_secs: 30,
            parallel_connections: 1,
            include_dns_time: true,
            good_speed_threshold_bps: 102_400,      // 100 KB/s
            acceptable_speed_threshold_bps: 51_200, // 50 KB/s
        }
    }
}

/// Quality rating for a speed test result
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpeedRating {
    /// Excellent speed (> 1 MB/s)
    Excellent,
    /// Good speed (> 100 KB/s)
    Good,
    /// Acceptable speed (> 50 KB/s)
    Acceptable,
    /// Slow speed (< 50 KB/s)
    Slow,
    /// Test failed (connection error, timeout, etc.)
    Failed,
}

impl std::fmt::Display for SpeedRating {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpeedRating::Excellent => write!(f, "🚀 Excellent"),
            SpeedRating::Good => write!(f, "✅ Good"),
            SpeedRating::Acceptable => write!(f, "⚠️ Acceptable"),
            SpeedRating::Slow => write!(f, "🐌 Slow"),
            SpeedRating::Failed => write!(f, "❌ Failed"),
        }
    }
}

/// Detailed timing breakdown of a speed test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestTiming {
    /// DNS resolution time
    pub dns_ms: f64,
    /// TCP connection time
    pub tcp_connect_ms: f64,
    /// TLS handshake time (0 if HTTP)
    pub tls_handshake_ms: f64,
    /// Time to first byte
    pub ttfb_ms: f64,
    /// Download transfer time (excluding connection setup)
    pub transfer_ms: f64,
    /// Total test duration
    pub total_ms: f64,
}

/// Result of a single speed test sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestResult {
    /// URL that was tested
    pub url: String,
    /// Whether the test succeeded
    pub success: bool,
    /// Error message if test failed
    pub error: Option<String>,
    /// Downloaded sample size in bytes
    pub sample_bytes: u64,
    /// Average download speed in bytes/sec
    pub speed_bps: f64,
    /// Speed rating
    pub rating: SpeedRating,
    /// Detailed timing breakdown
    pub timing: SpeedTestTiming,
    /// HTTP status code (if available)
    pub http_status: Option<u16>,
    /// Content-Type header (if available)
    pub content_type: Option<String>,
    /// Content-Length header (if available, indicates full file size)
    pub content_length: Option<u64>,
    /// Timestamp when the test was performed
    pub tested_at: chrono::DateTime<chrono::Utc>,
}

/// Summary of multiple speed test samples
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestSummary {
    /// Number of tests performed
    pub test_count: u32,
    /// Number of successful tests
    pub success_count: u32,
    /// Number of failed tests
    pub failed_count: u32,
    /// Average speed across all successful tests (bytes/sec)
    pub avg_speed_bps: f64,
    /// Minimum speed observed
    pub min_speed_bps: f64,
    /// Maximum speed observed
    pub max_speed_bps: f64,
    /// Median speed (P50)
    pub median_speed_bps: f64,
    /// Overall rating based on average speed
    pub overall_rating: SpeedRating,
    /// Estimated time to download a file of given size
    pub estimated_download_times: Vec<(u64, Duration)>,
    /// Recommended action
    pub recommendation: String,
}

/// Speed test manager that stores config and history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestManager {
    /// Current configuration
    config: SpeedTestConfig,
    /// History of test results (most recent first)
    history: Vec<SpeedTestResult>,
    /// Maximum history size
    max_history: usize,
}

impl Default for SpeedTestManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedTestManager {
    /// Create a new speed test manager with default config
    pub fn new() -> Self {
        Self {
            config: SpeedTestConfig::default(),
            history: Vec::new(),
            max_history: 50,
        }
    }

    /// Get current configuration
    pub fn get_config(&self) -> &SpeedTestConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: SpeedTestConfig) {
        self.config = config;
    }

    /// Get test history
    pub fn get_history(&self) -> &[SpeedTestResult] {
        &self.history
    }

    /// Get the most recent test result
    pub fn get_latest(&self) -> Option<&SpeedTestResult> {
        self.history.first()
    }

    /// Clear test history
    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    /// Record a test result
    pub fn record_result(&mut self, result: SpeedTestResult) {
        self.history.insert(0, result);
        if self.history.len() > self.max_history {
            self.history.truncate(self.max_history);
        }
    }

    /// Generate a summary from test history
    pub fn get_summary(&self) -> SpeedTestSummary {
        let successful: Vec<&SpeedTestResult> = self.history.iter().filter(|r| r.success).collect();

        let test_count = self.history.len() as u32;
        let success_count = successful.len() as u32;
        let failed_count = test_count - success_count;

        if successful.is_empty() {
            return SpeedTestSummary {
                test_count,
                success_count: 0,
                failed_count,
                avg_speed_bps: 0.0,
                min_speed_bps: 0.0,
                max_speed_bps: 0.0,
                median_speed_bps: 0.0,
                overall_rating: SpeedRating::Failed,
                estimated_download_times: Vec::new(),
                recommendation: "No successful speed tests recorded. Test a URL first.".to_string(),
            };
        }

        let mut speeds: Vec<f64> = successful.iter().map(|r| r.speed_bps).collect();
        speeds.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let avg_speed_bps = speeds.iter().sum::<f64>() / speeds.len() as f64;
        let min_speed_bps = speeds.first().copied().unwrap_or(0.0);
        let max_speed_bps = speeds.last().copied().unwrap_or(0.0);
        let median_speed_bps = if speeds.len() % 2 == 0 {
            (speeds[speeds.len() / 2 - 1] + speeds[speeds.len() / 2]) / 2.0
        } else {
            speeds[speeds.len() / 2]
        };

        let overall_rating = rate_speed(avg_speed_bps, &self.config);

        // Estimate download times for common file sizes
        let sizes = [
            (10 * 1024 * 1024, "10 MB"),
            (50 * 1024 * 1024, "50 MB"),
            (100 * 1024 * 1024, "100 MB"),
            (500 * 1024 * 1024, "500 MB"),
            (1024 * 1024 * 1024, "1 GB"),
        ];
        let estimated_download_times: Vec<(u64, Duration)> = sizes
            .iter()
            .map(|(bytes, _label)| {
                let secs = if avg_speed_bps > 0.0 {
                    (*bytes as f64 / avg_speed_bps) as u64
                } else {
                    u64::MAX
                };
                (*bytes, Duration::from_secs(secs))
            })
            .collect();

        let recommendation = generate_recommendation(avg_speed_bps, &self.config);

        SpeedTestSummary {
            test_count,
            success_count,
            failed_count,
            avg_speed_bps,
            min_speed_bps,
            max_speed_bps,
            median_speed_bps,
            overall_rating,
            estimated_download_times,
            recommendation,
        }
    }

    /// Perform a speed test to the given URL
    ///
    /// This downloads `sample_size_bytes` from the URL and measures throughput.
    /// Uses HTTP Range header to limit the download size.
    pub async fn test_url(&mut self, url: &str) -> SpeedTestResult {
        let config = self.config.clone();
        let result = perform_speed_test(url, &config).await;
        self.record_result(result.clone());
        result
    }

    /// Save configuration to disk
    pub fn save_config(&self, data_dir: &std::path::Path) -> Result<(), String> {
        let path = data_dir.join("speed_test_config.json");
        let json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| format!("serialize config: {}", e))?;
        std::fs::write(&path, json).map_err(|e| format!("write config: {}", e))?;
        Ok(())
    }

    /// Load configuration from disk
    pub fn load_config(&mut self, data_dir: &std::path::Path) -> Result<(), String> {
        let path = data_dir.join("speed_test_config.json");
        if !path.exists() {
            return Ok(());
        }
        let json = std::fs::read_to_string(&path).map_err(|e| format!("read config: {}", e))?;
        self.config = serde_json::from_str(&json).map_err(|e| format!("parse config: {}", e))?;
        Ok(())
    }

    /// Save history to disk
    pub fn save_history(&self, data_dir: &std::path::Path) -> Result<(), String> {
        let path = data_dir.join("speed_test_history.json");
        let json = serde_json::to_string_pretty(&self.history)
            .map_err(|e| format!("serialize history: {}", e))?;
        std::fs::write(&path, json).map_err(|e| format!("write history: {}", e))?;
        Ok(())
    }

    /// Load history from disk
    pub fn load_history(&mut self, data_dir: &std::path::Path) -> Result<(), String> {
        let path = data_dir.join("speed_test_history.json");
        if !path.exists() {
            return Ok(());
        }
        let json = std::fs::read_to_string(&path).map_err(|e| format!("read history: {}", e))?;
        self.history = serde_json::from_str(&json).map_err(|e| format!("parse history: {}", e))?;
        // Trim to max_history
        if self.history.len() > self.max_history {
            self.history.truncate(self.max_history);
        }
        Ok(())
    }
}

/// Rate a speed based on configuration thresholds
pub fn rate_speed(speed_bps: f64, config: &SpeedTestConfig) -> SpeedRating {
    if speed_bps <= 0.0 {
        return SpeedRating::Failed;
    }
    if speed_bps >= 1_048_576.0 {
        // > 1 MB/s
        SpeedRating::Excellent
    } else if speed_bps >= config.good_speed_threshold_bps as f64 {
        SpeedRating::Good
    } else if speed_bps >= config.acceptable_speed_threshold_bps as f64 {
        SpeedRating::Acceptable
    } else {
        SpeedRating::Slow
    }
}

/// Generate a recommendation based on measured speed
fn generate_recommendation(avg_speed_bps: f64, config: &SpeedTestConfig) -> String {
    if avg_speed_bps <= 0.0 {
        return "No speed data available. Test a URL to get started.".to_string();
    }

    let speed_kbps = avg_speed_bps / 1024.0;
    let speed_mbps = avg_speed_bps / (1024.0 * 1024.0);

    if speed_mbps >= 1.0 {
        format!(
            "Excellent connection ({:.1} MB/s). Safe to download large files.",
            speed_mbps
        )
    } else if speed_kbps >= config.good_speed_threshold_bps as f64 / 1024.0 {
        format!(
            "Good connection ({:.0} KB/s). Suitable for most downloads.",
            speed_kbps
        )
    } else if speed_kbps >= config.acceptable_speed_threshold_bps as f64 / 1024.0 {
        format!(
            "Acceptable connection ({:.0} KB/s). Consider downloading during off-peak hours for better speed.",
            speed_kbps
        )
    } else {
        format!(
            "Slow connection ({:.0} KB/s). Consider using a mirror, proxy, or waiting for better network conditions.",
            speed_kbps
        )
    }
}

/// Format bytes per second into a human-readable string
pub fn format_speed_bps(speed_bps: f64) -> String {
    if speed_bps >= 1_073_741_824.0 {
        format!("{:.2} GB/s", speed_bps / 1_073_741_824.0)
    } else if speed_bps >= 1_048_576.0 {
        format!("{:.2} MB/s", speed_bps / 1_048_576.0)
    } else if speed_bps >= 1024.0 {
        format!("{:.1} KB/s", speed_bps / 1024.0)
    } else {
        format!("{:.0} B/s", speed_bps)
    }
}

/// Format a duration into a human-readable ETA string
pub fn format_eta(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs == 0 {
        return "instant".to_string();
    }
    if secs >= 86400 {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        format!("{}d {}h", days, hours)
    } else if secs >= 3600 {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    } else if secs >= 60 {
        let mins = secs / 60;
        let remaining = secs % 60;
        format!("{}m {}s", mins, remaining)
    } else {
        format!("{}s", secs)
    }
}

/// Perform the actual speed test
async fn perform_speed_test(url: &str, config: &SpeedTestConfig) -> SpeedTestResult {
    let tested_at = chrono::Utc::now();
    let total_start = Instant::now();

    // Parse URL
    let parsed_url = match url::Url::parse(url) {
        Ok(u) => u,
        Err(e) => {
            return SpeedTestResult {
                url: url.to_string(),
                success: false,
                error: Some(format!("Invalid URL: {}", e)),
                sample_bytes: 0,
                speed_bps: 0.0,
                rating: SpeedRating::Failed,
                timing: SpeedTestTiming {
                    dns_ms: 0.0,
                    tcp_connect_ms: 0.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 0.0,
                    total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                },
                http_status: None,
                content_type: None,
                content_length: None,
                tested_at,
            };
        }
    };

    let scheme = parsed_url.scheme().to_string();
    if scheme != "http" && scheme != "https" {
        return SpeedTestResult {
            url: url.to_string(),
            success: false,
            error: Some(format!("Unsupported scheme: {} (only http/https)", scheme)),
            sample_bytes: 0,
            speed_bps: 0.0,
            rating: SpeedRating::Failed,
            timing: SpeedTestTiming {
                dns_ms: 0.0,
                tcp_connect_ms: 0.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 0.0,
                transfer_ms: 0.0,
                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
            },
            http_status: None,
            content_type: None,
            content_length: None,
            tested_at,
        };
    }

    // DNS resolution timing
    let dns_start = Instant::now();
    let host = parsed_url.host_str().unwrap_or("localhost");
    let port = parsed_url.port_or_known_default().unwrap_or(80);
    let addr_str = format!("{}:{}", host, port);

    if config.include_dns_time {
        match tokio::net::lookup_host(&addr_str).await {
            Ok(mut addrs) => {
                let dns_elapsed = dns_start.elapsed().as_secs_f64() * 1000.0;
                if let Some(addr) = addrs.next() {
                    // TCP connect
                    let tcp_start = Instant::now();
                    match tokio::time::timeout(
                        Duration::from_secs(config.timeout_secs),
                        tokio::net::TcpStream::connect(addr),
                    )
                    .await
                    {
                        Ok(Ok(_stream)) => {
                            let tcp_ms = tcp_start.elapsed().as_secs_f64() * 1000.0;
                            // For the actual HTTP test, use reqwest
                            perform_http_speed_test(
                                url,
                                config,
                                dns_elapsed,
                                tcp_ms,
                                total_start,
                                tested_at,
                            )
                            .await
                        }
                        Ok(Err(e)) => SpeedTestResult {
                            url: url.to_string(),
                            success: false,
                            error: Some(format!("TCP connect failed: {}", e)),
                            sample_bytes: 0,
                            speed_bps: 0.0,
                            rating: SpeedRating::Failed,
                            timing: SpeedTestTiming {
                                dns_ms: dns_elapsed,
                                tcp_connect_ms: tcp_start.elapsed().as_secs_f64() * 1000.0,
                                tls_handshake_ms: 0.0,
                                ttfb_ms: 0.0,
                                transfer_ms: 0.0,
                                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                            },
                            http_status: None,
                            content_type: None,
                            content_length: None,
                            tested_at,
                        },
                        Err(_) => SpeedTestResult {
                            url: url.to_string(),
                            success: false,
                            error: Some("Connection timed out".to_string()),
                            sample_bytes: 0,
                            speed_bps: 0.0,
                            rating: SpeedRating::Failed,
                            timing: SpeedTestTiming {
                                dns_ms: dns_elapsed,
                                tcp_connect_ms: tcp_start.elapsed().as_secs_f64() * 1000.0,
                                tls_handshake_ms: 0.0,
                                ttfb_ms: 0.0,
                                transfer_ms: 0.0,
                                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                            },
                            http_status: None,
                            content_type: None,
                            content_length: None,
                            tested_at,
                        },
                    }
                } else {
                    SpeedTestResult {
                        url: url.to_string(),
                        success: false,
                        error: Some("DNS resolved but no addresses".to_string()),
                        sample_bytes: 0,
                        speed_bps: 0.0,
                        rating: SpeedRating::Failed,
                        timing: SpeedTestTiming {
                            dns_ms: dns_elapsed,
                            tcp_connect_ms: 0.0,
                            tls_handshake_ms: 0.0,
                            ttfb_ms: 0.0,
                            transfer_ms: 0.0,
                            total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                        },
                        http_status: None,
                        content_type: None,
                        content_length: None,
                        tested_at,
                    }
                }
            }
            Err(e) => {
                let dns_elapsed = dns_start.elapsed().as_secs_f64() * 1000.0;
                SpeedTestResult {
                    url: url.to_string(),
                    success: false,
                    error: Some(format!("DNS resolution failed: {}", e)),
                    sample_bytes: 0,
                    speed_bps: 0.0,
                    rating: SpeedRating::Failed,
                    timing: SpeedTestTiming {
                        dns_ms: dns_elapsed,
                        tcp_connect_ms: 0.0,
                        tls_handshake_ms: 0.0,
                        ttfb_ms: 0.0,
                        transfer_ms: 0.0,
                        total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                    },
                    http_status: None,
                    content_type: None,
                    content_length: None,
                    tested_at,
                }
            }
        }
    } else {
        // Skip DNS timing, go straight to HTTP test
        perform_http_speed_test(url, config, 0.0, 0.0, total_start, tested_at).await
    }
}

/// Perform HTTP-based speed test using reqwest
async fn perform_http_speed_test(
    url: &str,
    config: &SpeedTestConfig,
    dns_ms: f64,
    tcp_connect_ms: f64,
    total_start: Instant,
    tested_at: chrono::DateTime<chrono::Utc>,
) -> SpeedTestResult {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return SpeedTestResult {
                url: url.to_string(),
                success: false,
                error: Some(format!("Failed to create HTTP client: {}", e)),
                sample_bytes: 0,
                speed_bps: 0.0,
                rating: SpeedRating::Failed,
                timing: SpeedTestTiming {
                    dns_ms,
                    tcp_connect_ms,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 0.0,
                    total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                },
                http_status: None,
                content_type: None,
                content_length: None,
                tested_at,
            };
        }
    };

    // Use Range header to limit download size
    let end_byte = config.sample_size_bytes.saturating_sub(1);
    let range_header = format!("bytes=0-{}", end_byte);

    let request_start = Instant::now();
    let response = match client.get(url).header("Range", &range_header).send().await {
        Ok(r) => r,
        Err(e) => {
            return SpeedTestResult {
                url: url.to_string(),
                success: false,
                error: Some(format!("HTTP request failed: {}", e)),
                sample_bytes: 0,
                speed_bps: 0.0,
                rating: SpeedRating::Failed,
                timing: SpeedTestTiming {
                    dns_ms,
                    tcp_connect_ms,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: request_start.elapsed().as_secs_f64() * 1000.0,
                    transfer_ms: 0.0,
                    total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
                },
                http_status: None,
                content_type: None,
                content_length: None,
                tested_at,
            };
        }
    };

    let http_status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let ttfb_ms = request_start.elapsed().as_secs_f64() * 1000.0;

    if !response.status().is_success() && http_status != 206 {
        return SpeedTestResult {
            url: url.to_string(),
            success: false,
            error: Some(format!("HTTP error: {}", response.status())),
            sample_bytes: 0,
            speed_bps: 0.0,
            rating: SpeedRating::Failed,
            timing: SpeedTestTiming {
                dns_ms,
                tcp_connect_ms,
                tls_handshake_ms: 0.0,
                ttfb_ms,
                transfer_ms: 0.0,
                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
            },
            http_status: Some(http_status),
            content_type,
            content_length,
            tested_at,
        };
    }

    // Download the response body and measure throughput
    let transfer_start = Instant::now();
    match response.bytes().await {
        Ok(bytes) => {
            let transfer_ms = transfer_start.elapsed().as_secs_f64() * 1000.0;
            let sample_bytes = bytes.len() as u64;
            let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;

            let speed_bps = if transfer_ms > 0.0 {
                (sample_bytes as f64 / transfer_ms) * 1000.0
            } else {
                0.0
            };

            let rating = rate_speed(speed_bps, config);
            let tls_handshake_ms = 0.0; // Cannot easily measure separately with reqwest

            SpeedTestResult {
                url: url.to_string(),
                success: true,
                error: None,
                sample_bytes,
                speed_bps,
                rating,
                timing: SpeedTestTiming {
                    dns_ms,
                    tcp_connect_ms,
                    tls_handshake_ms,
                    ttfb_ms,
                    transfer_ms,
                    total_ms,
                },
                http_status: Some(http_status),
                content_type,
                content_length,
                tested_at,
            }
        }
        Err(e) => SpeedTestResult {
            url: url.to_string(),
            success: false,
            error: Some(format!("Failed to read response body: {}", e)),
            sample_bytes: 0,
            speed_bps: 0.0,
            rating: SpeedRating::Failed,
            timing: SpeedTestTiming {
                dns_ms,
                tcp_connect_ms,
                tls_handshake_ms: 0.0,
                ttfb_ms,
                transfer_ms: transfer_start.elapsed().as_secs_f64() * 1000.0,
                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
            },
            http_status: Some(http_status),
            content_type,
            content_length: None,
            tested_at,
        },
    }
}

/// Format a speed test result for display
pub fn format_result(result: &SpeedTestResult) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Speed Test: {}", result.url));
    lines.push(format!("  Rating: {}", result.rating));

    if result.success {
        lines.push(format!("  Speed: {}", format_speed_bps(result.speed_bps)));
        lines.push(format!(
            "  Sample: {} bytes transferred",
            result.sample_bytes
        ));
        lines.push(format!(
            "  Timing: DNS={:.0}ms TCP={:.0}ms TTFB={:.0}ms Transfer={:.0}ms Total={:.0}ms",
            result.timing.dns_ms,
            result.timing.tcp_connect_ms,
            result.timing.ttfb_ms,
            result.timing.transfer_ms,
            result.timing.total_ms
        ));
        if let Some(status) = result.http_status {
            lines.push(format!("  HTTP Status: {}", status));
        }
        if let Some(ct) = &result.content_type {
            lines.push(format!("  Content-Type: {}", ct));
        }
        if let Some(cl) = result.content_length {
            lines.push(format!("  Content-Length: {} bytes", cl));
        }
    } else {
        if let Some(err) = &result.error {
            lines.push(format!("  Error: {}", err));
        }
    }

    lines.join("\n")
}

/// Format a speed test summary for display
pub fn format_summary(summary: &SpeedTestSummary) -> String {
    let mut lines = Vec::new();
    lines.push("═══ Speed Test Summary ═══".to_string());
    lines.push(format!(
        "Tests: {} total, {} success, {} failed",
        summary.test_count, summary.success_count, summary.failed_count
    ));

    if summary.success_count > 0 {
        lines.push(format!(
            "Average Speed: {} ({} overall)",
            format_speed_bps(summary.avg_speed_bps),
            summary.overall_rating
        ));
        lines.push(format!(
            "Range: {} ~ {}",
            format_speed_bps(summary.min_speed_bps),
            format_speed_bps(summary.max_speed_bps)
        ));
        lines.push(format!(
            "Median: {}",
            format_speed_bps(summary.median_speed_bps)
        ));
        lines.push(String::new());
        lines.push("Estimated Download Times:".to_string());
        let size_labels = ["10 MB", "50 MB", "100 MB", "500 MB", "1 GB"];
        for (i, (_bytes, duration)) in summary.estimated_download_times.iter().enumerate() {
            let label = size_labels.get(i).unwrap_or(&"?");
            lines.push(format!("  {}: {}", label, format_eta(*duration)));
        }
        lines.push(String::new());
        lines.push(format!("💡 {}", summary.recommendation));
    } else {
        lines.push("No successful tests to analyze.".to_string());
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SpeedTestConfig::default();
        assert_eq!(config.sample_size_bytes, 1_048_576);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.parallel_connections, 1);
        assert!(config.include_dns_time);
        assert_eq!(config.good_speed_threshold_bps, 102_400);
        assert_eq!(config.acceptable_speed_threshold_bps, 51_200);
    }

    #[test]
    fn test_config_serialization() {
        let config = SpeedTestConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SpeedTestConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sample_size_bytes, config.sample_size_bytes);
        assert_eq!(deserialized.timeout_secs, config.timeout_secs);
    }

    #[test]
    fn test_rate_speed_excellent() {
        let config = SpeedTestConfig::default();
        assert_eq!(rate_speed(2_000_000.0, &config), SpeedRating::Excellent);
        assert_eq!(rate_speed(1_048_576.0, &config), SpeedRating::Excellent);
    }

    #[test]
    fn test_rate_speed_good() {
        let config = SpeedTestConfig::default();
        assert_eq!(rate_speed(200_000.0, &config), SpeedRating::Good);
        assert_eq!(rate_speed(102_400.0, &config), SpeedRating::Good);
    }

    #[test]
    fn test_rate_speed_acceptable() {
        let config = SpeedTestConfig::default();
        assert_eq!(rate_speed(75_000.0, &config), SpeedRating::Acceptable);
        assert_eq!(rate_speed(51_200.0, &config), SpeedRating::Acceptable);
    }

    #[test]
    fn test_rate_speed_slow() {
        let config = SpeedTestConfig::default();
        assert_eq!(rate_speed(10_000.0, &config), SpeedRating::Slow);
        assert_eq!(rate_speed(1.0, &config), SpeedRating::Slow);
    }

    #[test]
    fn test_rate_speed_failed() {
        let config = SpeedTestConfig::default();
        assert_eq!(rate_speed(0.0, &config), SpeedRating::Failed);
        assert_eq!(rate_speed(-1.0, &config), SpeedRating::Failed);
    }

    #[test]
    fn test_speed_rating_display() {
        assert_eq!(format!("{}", SpeedRating::Excellent), "🚀 Excellent");
        assert_eq!(format!("{}", SpeedRating::Good), "✅ Good");
        assert_eq!(format!("{}", SpeedRating::Acceptable), "⚠️ Acceptable");
        assert_eq!(format!("{}", SpeedRating::Slow), "🐌 Slow");
        assert_eq!(format!("{}", SpeedRating::Failed), "❌ Failed");
    }

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(500.0), "500 B/s");
        assert_eq!(format_speed_bps(1024.0), "1.0 KB/s");
        assert_eq!(format_speed_bps(1536.0), "1.5 KB/s");
        assert_eq!(format_speed_bps(1_048_576.0), "1.00 MB/s");
        assert_eq!(format_speed_bps(1_073_741_824.0), "1.00 GB/s");
    }

    #[test]
    fn test_format_eta() {
        assert_eq!(format_eta(Duration::from_secs(0)), "instant");
        assert_eq!(format_eta(Duration::from_secs(30)), "30s");
        assert_eq!(format_eta(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_eta(Duration::from_secs(3661)), "1h 1m");
        assert_eq!(format_eta(Duration::from_secs(90000)), "1d 1h");
    }

    #[test]
    fn test_manager_new() {
        let manager = SpeedTestManager::new();
        assert!(manager.get_history().is_empty());
        assert!(manager.get_latest().is_none());
        assert_eq!(manager.get_config().sample_size_bytes, 1_048_576);
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = SpeedTestManager::new();
        let mut config = SpeedTestConfig::default();
        config.sample_size_bytes = 2_097_152;
        manager.set_config(config);
        assert_eq!(manager.get_config().sample_size_bytes, 2_097_152);
    }

    #[test]
    fn test_manager_record_result() {
        let mut manager = SpeedTestManager::new();
        let result = SpeedTestResult {
            url: "http://example.com/file.zip".to_string(),
            success: true,
            error: None,
            sample_bytes: 1_048_576,
            speed_bps: 500_000.0,
            rating: SpeedRating::Good,
            timing: SpeedTestTiming {
                dns_ms: 10.0,
                tcp_connect_ms: 20.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 50.0,
                transfer_ms: 1000.0,
                total_ms: 1080.0,
            },
            http_status: Some(206),
            content_type: Some("application/octet-stream".to_string()),
            content_length: Some(10_485_760),
            tested_at: chrono::Utc::now(),
        };
        manager.record_result(result);
        assert_eq!(manager.get_history().len(), 1);
        assert!(manager.get_latest().is_some());
        assert_eq!(
            manager.get_latest().unwrap().url,
            "http://example.com/file.zip"
        );
    }

    #[test]
    fn test_manager_history_order() {
        let mut manager = SpeedTestManager::new();
        for i in 0..3 {
            let result = SpeedTestResult {
                url: format!("http://example{}.com", i),
                success: true,
                error: None,
                sample_bytes: 100,
                speed_bps: 1000.0 * (i as f64 + 1.0),
                rating: SpeedRating::Slow,
                timing: SpeedTestTiming {
                    dns_ms: 0.0,
                    tcp_connect_ms: 0.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 100.0,
                    total_ms: 100.0,
                },
                http_status: None,
                content_type: None,
                content_length: None,
                tested_at: chrono::Utc::now(),
            };
            manager.record_result(result);
        }
        // Most recent first
        assert_eq!(manager.get_history()[0].url, "http://example2.com");
        assert_eq!(manager.get_history()[2].url, "http://example0.com");
    }

    #[test]
    fn test_manager_max_history() {
        let mut manager = SpeedTestManager::new();
        manager.max_history = 5;
        for i in 0..10 {
            let result = SpeedTestResult {
                url: format!("http://example{}.com", i),
                success: true,
                error: None,
                sample_bytes: 100,
                speed_bps: 1000.0,
                rating: SpeedRating::Slow,
                timing: SpeedTestTiming {
                    dns_ms: 0.0,
                    tcp_connect_ms: 0.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 100.0,
                    total_ms: 100.0,
                },
                http_status: None,
                content_type: None,
                content_length: None,
                tested_at: chrono::Utc::now(),
            };
            manager.record_result(result);
        }
        assert_eq!(manager.get_history().len(), 5);
        // Most recent should be the last one added
        assert_eq!(manager.get_history()[0].url, "http://example9.com");
    }

    #[test]
    fn test_manager_clear_history() {
        let mut manager = SpeedTestManager::new();
        let result = SpeedTestResult {
            url: "http://example.com".to_string(),
            success: true,
            error: None,
            sample_bytes: 100,
            speed_bps: 1000.0,
            rating: SpeedRating::Slow,
            timing: SpeedTestTiming {
                dns_ms: 0.0,
                tcp_connect_ms: 0.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 0.0,
                transfer_ms: 100.0,
                total_ms: 100.0,
            },
            http_status: None,
            content_type: None,
            content_length: None,
            tested_at: chrono::Utc::now(),
        };
        manager.record_result(result);
        assert_eq!(manager.get_history().len(), 1);
        manager.clear_history();
        assert!(manager.get_history().is_empty());
    }

    #[test]
    fn test_summary_empty() {
        let manager = SpeedTestManager::new();
        let summary = manager.get_summary();
        assert_eq!(summary.test_count, 0);
        assert_eq!(summary.success_count, 0);
        assert_eq!(summary.failed_count, 0);
        assert_eq!(summary.avg_speed_bps, 0.0);
        assert_eq!(summary.overall_rating, SpeedRating::Failed);
        assert!(summary.estimated_download_times.is_empty());
    }

    #[test]
    fn test_summary_all_failed() {
        let mut manager = SpeedTestManager::new();
        for i in 0..3 {
            let result = SpeedTestResult {
                url: format!("http://fail{}.com", i),
                success: false,
                error: Some("Connection refused".to_string()),
                sample_bytes: 0,
                speed_bps: 0.0,
                rating: SpeedRating::Failed,
                timing: SpeedTestTiming {
                    dns_ms: 0.0,
                    tcp_connect_ms: 0.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 0.0,
                    total_ms: 100.0,
                },
                http_status: None,
                content_type: None,
                content_length: None,
                tested_at: chrono::Utc::now(),
            };
            manager.record_result(result);
        }
        let summary = manager.get_summary();
        assert_eq!(summary.test_count, 3);
        assert_eq!(summary.success_count, 0);
        assert_eq!(summary.failed_count, 3);
        assert_eq!(summary.overall_rating, SpeedRating::Failed);
    }

    #[test]
    fn test_summary_with_successful_tests() {
        let mut manager = SpeedTestManager::new();
        let speeds = [200_000.0, 500_000.0, 1_000_000.0];
        for (i, &speed) in speeds.iter().enumerate() {
            let result = SpeedTestResult {
                url: format!("http://test{}.com", i),
                success: true,
                error: None,
                sample_bytes: 1_048_576,
                speed_bps: speed,
                rating: rate_speed(speed, manager.get_config()),
                timing: SpeedTestTiming {
                    dns_ms: 10.0,
                    tcp_connect_ms: 20.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 50.0,
                    transfer_ms: 1000.0,
                    total_ms: 1080.0,
                },
                http_status: Some(200),
                content_type: None,
                content_length: None,
                tested_at: chrono::Utc::now(),
            };
            manager.record_result(result);
        }
        let summary = manager.get_summary();
        assert_eq!(summary.test_count, 3);
        assert_eq!(summary.success_count, 3);
        assert_eq!(summary.failed_count, 0);
        // avg = (200k + 500k + 1M) / 3 = 566,666.67
        assert!((summary.avg_speed_bps - 566_666.6666).abs() < 1.0);
        assert_eq!(summary.min_speed_bps, 200_000.0);
        assert_eq!(summary.max_speed_bps, 1_000_000.0);
        assert_eq!(summary.median_speed_bps, 500_000.0);
        assert_eq!(summary.overall_rating, SpeedRating::Good);
        assert_eq!(summary.estimated_download_times.len(), 5);
    }

    #[test]
    fn test_summary_mixed_success_and_failure() {
        let mut manager = SpeedTestManager::new();
        // 2 successful
        for i in 0..2 {
            let result = SpeedTestResult {
                url: format!("http://ok{}.com", i),
                success: true,
                error: None,
                sample_bytes: 100_000,
                speed_bps: 300_000.0,
                rating: SpeedRating::Good,
                timing: SpeedTestTiming {
                    dns_ms: 0.0,
                    tcp_connect_ms: 0.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 100.0,
                    total_ms: 100.0,
                },
                http_status: Some(200),
                content_type: None,
                content_length: None,
                tested_at: chrono::Utc::now(),
            };
            manager.record_result(result);
        }
        // 1 failed
        let fail = SpeedTestResult {
            url: "http://fail.com".to_string(),
            success: false,
            error: Some("timeout".to_string()),
            sample_bytes: 0,
            speed_bps: 0.0,
            rating: SpeedRating::Failed,
            timing: SpeedTestTiming {
                dns_ms: 0.0,
                tcp_connect_ms: 0.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 0.0,
                transfer_ms: 0.0,
                total_ms: 30000.0,
            },
            http_status: None,
            content_type: None,
            content_length: None,
            tested_at: chrono::Utc::now(),
        };
        manager.record_result(fail);

        let summary = manager.get_summary();
        assert_eq!(summary.test_count, 3);
        assert_eq!(summary.success_count, 2);
        assert_eq!(summary.failed_count, 1);
    }

    #[test]
    fn test_generate_recommendation_excellent() {
        let config = SpeedTestConfig::default();
        let rec = generate_recommendation(2_000_000.0, &config);
        assert!(rec.contains("Excellent"));
    }

    #[test]
    fn test_generate_recommendation_good() {
        let config = SpeedTestConfig::default();
        let rec = generate_recommendation(200_000.0, &config);
        assert!(rec.contains("Good"));
    }

    #[test]
    fn test_generate_recommendation_acceptable() {
        let config = SpeedTestConfig::default();
        let rec = generate_recommendation(75_000.0, &config);
        assert!(rec.contains("Acceptable"));
    }

    #[test]
    fn test_generate_recommendation_slow() {
        let config = SpeedTestConfig::default();
        let rec = generate_recommendation(10_000.0, &config);
        assert!(rec.contains("Slow"));
    }

    #[test]
    fn test_generate_recommendation_no_data() {
        let config = SpeedTestConfig::default();
        let rec = generate_recommendation(0.0, &config);
        assert!(rec.contains("No speed data"));
    }

    #[test]
    fn test_format_result_success() {
        let result = SpeedTestResult {
            url: "http://example.com/file.zip".to_string(),
            success: true,
            error: None,
            sample_bytes: 1_048_576,
            speed_bps: 500_000.0,
            rating: SpeedRating::Good,
            timing: SpeedTestTiming {
                dns_ms: 10.0,
                tcp_connect_ms: 20.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 50.0,
                transfer_ms: 1000.0,
                total_ms: 1080.0,
            },
            http_status: Some(206),
            content_type: Some("application/octet-stream".to_string()),
            content_length: Some(10_485_760),
            tested_at: chrono::Utc::now(),
        };
        let formatted = format_result(&result);
        assert!(formatted.contains("http://example.com/file.zip"));
        assert!(formatted.contains("Good"));
        assert!(formatted.contains("488.3 KB/s")); // 500000 B/s
        assert!(formatted.contains("DNS=10ms"));
        assert!(formatted.contains("HTTP Status: 206"));
    }

    #[test]
    fn test_format_result_failure() {
        let result = SpeedTestResult {
            url: "http://fail.com".to_string(),
            success: false,
            error: Some("Connection refused".to_string()),
            sample_bytes: 0,
            speed_bps: 0.0,
            rating: SpeedRating::Failed,
            timing: SpeedTestTiming {
                dns_ms: 0.0,
                tcp_connect_ms: 0.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 0.0,
                transfer_ms: 0.0,
                total_ms: 100.0,
            },
            http_status: None,
            content_type: None,
            content_length: None,
            tested_at: chrono::Utc::now(),
        };
        let formatted = format_result(&result);
        assert!(formatted.contains("Connection refused"));
        assert!(formatted.contains("Failed"));
    }

    #[test]
    fn test_format_summary() {
        let summary = SpeedTestSummary {
            test_count: 5,
            success_count: 4,
            failed_count: 1,
            avg_speed_bps: 500_000.0,
            min_speed_bps: 100_000.0,
            max_speed_bps: 1_000_000.0,
            median_speed_bps: 450_000.0,
            overall_rating: SpeedRating::Good,
            estimated_download_times: vec![
                (10 * 1024 * 1024, Duration::from_secs(21)),
                (50 * 1024 * 1024, Duration::from_secs(105)),
                (100 * 1024 * 1024, Duration::from_secs(210)),
                (500 * 1024 * 1024, Duration::from_secs(1049)),
                (1024 * 1024 * 1024, Duration::from_secs(2150)),
            ],
            recommendation: "Good connection (488.3 KB/s). Suitable for most downloads."
                .to_string(),
        };
        let formatted = format_summary(&summary);
        assert!(formatted.contains("Speed Test Summary"));
        assert!(formatted.contains("5 total, 4 success, 1 failed"));
        assert!(formatted.contains("488.3 KB/s"));
        assert!(formatted.contains("10 MB"));
        assert!(formatted.contains("1 GB"));
    }

    #[test]
    fn test_format_summary_empty() {
        let summary = SpeedTestSummary {
            test_count: 0,
            success_count: 0,
            failed_count: 0,
            avg_speed_bps: 0.0,
            min_speed_bps: 0.0,
            max_speed_bps: 0.0,
            median_speed_bps: 0.0,
            overall_rating: SpeedRating::Failed,
            estimated_download_times: vec![],
            recommendation: "No successful speed tests recorded.".to_string(),
        };
        let formatted = format_summary(&summary);
        assert!(formatted.contains("No successful tests"));
    }

    #[test]
    fn test_save_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = SpeedTestManager::new();
        let mut config = SpeedTestConfig::default();
        config.sample_size_bytes = 2_097_152;
        config.timeout_secs = 60;
        manager.set_config(config);
        manager.save_config(dir.path()).unwrap();

        let mut loaded = SpeedTestManager::new();
        loaded.load_config(dir.path()).unwrap();
        assert_eq!(loaded.get_config().sample_size_bytes, 2_097_152);
        assert_eq!(loaded.get_config().timeout_secs, 60);
    }

    #[test]
    fn test_save_load_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = SpeedTestManager::new();
        let result = SpeedTestResult {
            url: "http://example.com/test".to_string(),
            success: true,
            error: None,
            sample_bytes: 1024,
            speed_bps: 50_000.0,
            rating: SpeedRating::Acceptable,
            timing: SpeedTestTiming {
                dns_ms: 5.0,
                tcp_connect_ms: 10.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 20.0,
                transfer_ms: 100.0,
                total_ms: 135.0,
            },
            http_status: Some(200),
            content_type: None,
            content_length: None,
            tested_at: chrono::Utc::now(),
        };
        manager.record_result(result);
        manager.save_history(dir.path()).unwrap();

        let mut loaded = SpeedTestManager::new();
        loaded.load_history(dir.path()).unwrap();
        assert_eq!(loaded.get_history().len(), 1);
        assert_eq!(loaded.get_history()[0].url, "http://example.com/test");
    }

    #[test]
    fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = SpeedTestManager::new();
        // Should succeed even if file doesn't exist
        assert!(manager.load_config(dir.path()).is_ok());
    }

    #[test]
    fn test_load_history_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = SpeedTestManager::new();
        assert!(manager.load_history(dir.path()).is_ok());
        assert!(manager.get_history().is_empty());
    }

    #[tokio::test]
    async fn test_test_url_invalid_url() {
        let mut manager = SpeedTestManager::new();
        let result = manager.test_url("not a valid url at all %%").await;
        assert!(!result.success);
        assert!(result.error.is_some());
        assert_eq!(result.rating, SpeedRating::Failed);
    }

    #[tokio::test]
    async fn test_test_url_unsupported_scheme() {
        let mut manager = SpeedTestManager::new();
        let result = manager.test_url("ftp://example.com/file").await;
        assert!(!result.success);
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("Unsupported scheme"));
    }

    #[tokio::test]
    async fn test_test_url_records_to_history() {
        let mut manager = SpeedTestManager::new();
        // Even failed tests should be recorded
        let _ = manager.test_url("http://nonexistent.invalid/test").await;
        assert_eq!(manager.get_history().len(), 1);
    }

    #[test]
    fn test_speed_test_timing_serialization() {
        let timing = SpeedTestTiming {
            dns_ms: 10.5,
            tcp_connect_ms: 20.3,
            tls_handshake_ms: 15.0,
            ttfb_ms: 50.0,
            transfer_ms: 1000.0,
            total_ms: 1095.8,
        };
        let json = serde_json::to_string(&timing).unwrap();
        let deserialized: SpeedTestTiming = serde_json::from_str(&json).unwrap();
        assert!((deserialized.dns_ms - 10.5).abs() < 0.01);
        assert!((deserialized.total_ms - 1095.8).abs() < 0.01);
    }

    #[test]
    fn test_speed_test_result_serialization() {
        let result = SpeedTestResult {
            url: "http://example.com".to_string(),
            success: true,
            error: None,
            sample_bytes: 1_048_576,
            speed_bps: 500_000.0,
            rating: SpeedRating::Good,
            timing: SpeedTestTiming {
                dns_ms: 10.0,
                tcp_connect_ms: 20.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 50.0,
                transfer_ms: 1000.0,
                total_ms: 1080.0,
            },
            http_status: Some(206),
            content_type: Some("application/zip".to_string()),
            content_length: Some(10_000_000),
            tested_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SpeedTestResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.url, "http://example.com");
        assert!(deserialized.success);
        assert_eq!(deserialized.rating, SpeedRating::Good);
        assert_eq!(deserialized.http_status, Some(206));
    }

    #[test]
    fn test_summary_estimated_times() {
        let mut manager = SpeedTestManager::new();
        // Record a test at exactly 1 MB/s
        let result = SpeedTestResult {
            url: "http://fast.com".to_string(),
            success: true,
            error: None,
            sample_bytes: 1_048_576,
            speed_bps: 1_048_576.0,
            rating: SpeedRating::Excellent,
            timing: SpeedTestTiming {
                dns_ms: 0.0,
                tcp_connect_ms: 0.0,
                tls_handshake_ms: 0.0,
                ttfb_ms: 0.0,
                transfer_ms: 1000.0,
                total_ms: 1000.0,
            },
            http_status: Some(200),
            content_type: None,
            content_length: None,
            tested_at: chrono::Utc::now(),
        };
        manager.record_result(result);
        let summary = manager.get_summary();
        // At 1 MB/s, 10 MB should take ~10 seconds
        assert_eq!(summary.estimated_download_times.len(), 5);
        let ten_mb = &summary.estimated_download_times[0];
        assert_eq!(ten_mb.0, 10 * 1024 * 1024);
        assert!(ten_mb.1.as_secs() <= 11); // ~10s
    }

    #[test]
    fn test_median_even_count() {
        let mut manager = SpeedTestManager::new();
        // Add 4 results: 100k, 200k, 300k, 400k
        for speed in [100_000.0, 200_000.0, 300_000.0, 400_000.0] {
            let result = SpeedTestResult {
                url: format!("http://test-{}.com", speed as u64),
                success: true,
                error: None,
                sample_bytes: 1000,
                speed_bps: speed,
                rating: SpeedRating::Good,
                timing: SpeedTestTiming {
                    dns_ms: 0.0,
                    tcp_connect_ms: 0.0,
                    tls_handshake_ms: 0.0,
                    ttfb_ms: 0.0,
                    transfer_ms: 100.0,
                    total_ms: 100.0,
                },
                http_status: Some(200),
                content_type: None,
                content_length: None,
                tested_at: chrono::Utc::now(),
            };
            manager.record_result(result);
        }
        let summary = manager.get_summary();
        // Median of [100k, 200k, 300k, 400k] = (200k + 300k) / 2 = 250k
        assert!((summary.median_speed_bps - 250_000.0).abs() < 1.0);
    }
}
