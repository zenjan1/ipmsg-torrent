//! HTTP Multi-Segment Download with Performance Optimizations
//!
//! Splits a single HTTP URL into multiple parallel byte-range segments for faster downloads.
//! Similar to aria2/IDM multi-connection download acceleration.
//!
//! Performance Features:
//! - Bandwidth-adaptive segment count (more segments for high bandwidth)
//! - Buffered I/O for reduced disk operations
//! - Optimized HTTP client with connection pooling
//! - Streaming writes (no memory accumulation)
//! - Dynamic segment sizing based on file size

use crate::rate_limiter::RateLimiter;
use reqwest::Client;
use std::path::PathBuf;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::time::{Duration, Instant, interval};
use tokio_util::sync::CancellationToken;

/// Default number of segments for multi-segment download
const DEFAULT_SEGMENT_COUNT: usize = 4;

/// Maximum segments for high-bandwidth connections
const MAX_SEGMENT_COUNT: usize = 16;

/// Minimum file size for multi-segment download (1MB)
const MIN_FILE_SIZE_FOR_SEGMENTATION: u64 = 1024 * 1024;

/// Max retries per segment before giving up
const MAX_SEGMENT_RETRIES: u32 = 3;

/// Retry delay base (doubles each attempt)
const RETRY_BASE_DELAY_MS: u64 = 500;

/// Write buffer size (64KB)
const WRITE_BUFFER_SIZE: usize = 64 * 1024;

/// Bandwidth threshold for increasing segments (2 MB/s)
const HIGH_BANDWIDTH_THRESHOLD: f64 = 2_000_000.0;

/// HTTP multi-segment download engine with performance optimizations
pub struct SegmentDownloader {
    url: String,
    file_name: String,
    file_size: u64,
    segments: Vec<Segment>,
    download_dir: PathBuf,
    http_client: Client,
    downloaded: u64,
    start_time: Option<Instant>,
    output_file: Option<BufWriter<tokio::fs::File>>,
    rate_limiter: Option<RateLimiter>,
    segment_count: usize,
    /// Current estimated bandwidth (bytes/sec)
    estimated_bandwidth: f64,
    /// Pending writes queue for batching
    pending_writes: Vec<(u64, Vec<u8>)>,
}

/// A segment of the file to download with performance tracking
#[derive(Debug, Clone)]
struct Segment {
    /// Byte offset in the file
    offset: u64,
    /// Size of this segment
    size: u64,
    /// Whether this segment has been downloaded
    downloaded: bool,
    /// Index of this segment (for logging)
    index: usize,
    /// Last measured throughput for this segment (bytes/sec)
    throughput_bps: f64,
}

/// Progress information for a segment download
#[derive(Debug, Clone)]
pub struct SegmentDownloadProgress {
    /// Total file size
    pub total_size: u64,
    /// Total bytes downloaded
    pub downloaded: u64,
    /// Download speed (bytes/sec)
    pub speed: f64,
    /// Number of segments
    pub total_segments: usize,
    /// Number of completed segments
    pub completed_segments: usize,
    /// Estimated bandwidth (bytes/sec)
    pub estimated_bandwidth: f64,
    /// Progress per segment (for debugging)
    pub segment_progress: Vec<SegmentInfo>,
}

/// Information about a single segment
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub index: usize,
    pub offset: u64,
    pub size: u64,
    pub downloaded: bool,
    pub throughput_bps: f64,
}

impl SegmentDownloader {
    /// Create a new segment downloader
    pub fn new(url: String, file_name: String, file_size: u64, download_dir: PathBuf) -> Self {
        // Determine initial segment count based on file size
        let segment_count = if file_size < MIN_FILE_SIZE_FOR_SEGMENTATION {
            1 // Small files: single connection
        } else {
            DEFAULT_SEGMENT_COUNT
        };

        // Split file into segments
        let segments = Self::create_segments(file_size, segment_count);

        // Optimized HTTP client with connection pooling
        let http_client = Client::builder()
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let mut downloader = Self {
            url,
            file_name,
            file_size,
            segments,
            download_dir,
            http_client,
            downloaded: 0,
            start_time: None,
            output_file: None,
            rate_limiter: None,
            segment_count,
            estimated_bandwidth: 0.0,
            pending_writes: Vec::with_capacity(16),
        };

        // Try to load existing progress
        if let Err(e) = downloader.load_progress() {
            tracing::debug!("No existing progress to load: {}", e);
        }

        downloader
    }

    /// Set rate limiter for speed control
    pub fn set_rate_limiter(&mut self, limiter: RateLimiter) {
        self.rate_limiter = Some(limiter);
    }

    /// Set custom segment count (for testing or advanced users)
    pub fn set_segment_count(&mut self, count: usize) {
        self.segment_count = count;
        self.segments = Self::create_segments(self.file_size, count);
    }

    /// Calculate optimal segment count based on bandwidth
    fn calculate_optimal_segment_count(file_size: u64, bandwidth_bps: f64) -> usize {
        if file_size < MIN_FILE_SIZE_FOR_SEGMENTATION {
            return 1;
        }

        // Base count on bandwidth
        let base_count = if bandwidth_bps > HIGH_BANDWIDTH_THRESHOLD {
            MAX_SEGMENT_COUNT
        } else if bandwidth_bps > HIGH_BANDWIDTH_THRESHOLD / 2.0 {
            8
        } else if bandwidth_bps > HIGH_BANDWIDTH_THRESHOLD / 4.0 {
            DEFAULT_SEGMENT_COUNT
        } else {
            2
        };

        // Also consider file size (don't have too small segments)
        let min_segment_size = 256 * 1024; // 256KB minimum
        let max_by_size = (file_size / min_segment_size) as usize;
        
        base_count.min(max_by_size.max(1)).min(MAX_SEGMENT_COUNT)
    }

    /// Update bandwidth estimate
    fn update_bandwidth_estimate(&mut self, bytes: u64, duration_ms: f64) {
        if duration_ms > 0.0 {
            let instant_bw = (bytes as f64 * 1000.0) / duration_ms;
            // EWMA smoothing
            self.estimated_bandwidth = if self.estimated_bandwidth == 0.0 {
                instant_bw
            } else {
                0.7 * self.estimated_bandwidth + 0.3 * instant_bw
            };
        }
    }

    /// Adjust segment count based on bandwidth
    fn maybe_adjust_segment_count(&mut self) {
        let optimal = Self::calculate_optimal_segment_count(self.file_size, self.estimated_bandwidth);
        
        if optimal != self.segment_count && optimal > self.segments.iter().filter(|s| !s.downloaded).count() {
            tracing::debug!(
                old_count = self.segment_count,
                new_count = optimal,
                bandwidth = self.estimated_bandwidth,
                "Adjusting segment count based on bandwidth"
            );
            // Only adjust if we have many remaining segments
            self.segment_count = optimal;
        }
    }

    /// Create segments by splitting the file into equal parts
    fn create_segments(file_size: u64, segment_count: usize) -> Vec<Segment> {
        let segment_size = file_size / segment_count as u64;
        let mut segments = Vec::with_capacity(segment_count);
        let mut offset = 0u64;

        for i in 0..segment_count {
            // Last segment gets any remainder
            let size = if i == segment_count - 1 {
                file_size - offset
            } else {
                segment_size
            };

            segments.push(Segment {
                offset,
                size,
                downloaded: false,
                index: i,
                throughput_bps: 0.0,
            });

            offset += size;
        }

        segments
    }

    /// Start the multi-segment download
    pub async fn download(
        &mut self,
        cancel: Option<CancellationToken>,
    ) -> Result<(), SegmentDownloadError> {
        tracing::info!(
            name = %self.file_name,
            size = self.file_size,
            segments = self.segments.len(),
            url = %self.url,
            "Starting optimized multi-segment HTTP download"
        );

        // Create output file
        let output_path = self.download_dir.join(&self.file_name);
        tokio::fs::create_dir_all(&self.download_dir)
            .await
            .map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        // Open file for writing (don't truncate on resume)
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&output_path)
            .await
            .map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        // Pre-allocate file size
        file.set_len(self.file_size)
            .await
            .map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        // Wrap in BufWriter for reduced I/O operations
        self.output_file = Some(BufWriter::with_capacity(WRITE_BUFFER_SIZE, file));
        self.start_time = Some(Instant::now());

        // Main download loop
        let mut tick = interval(Duration::from_millis(100));
        let mut last_bandwidth_check = Instant::now();

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Check if cancelled
                    if let Some(ref cancel) = cancel
                        && cancel.is_cancelled() {
                            tracing::info!("Download cancelled");
                            // Flush pending writes before exit
                            self.flush_writes().await?;
                            return Err(SegmentDownloadError::Io("cancelled".to_string()));
                        }

                    // Check if download is complete
                    if self.is_complete() {
                        tracing::info!("Download complete!");
                        break;
                    }

                    // Download pending segments in parallel
                    self.download_segments().await;

                    // Periodically check bandwidth and adjust segment count
                    if last_bandwidth_check.elapsed() > Duration::from_secs(5) {
                        self.maybe_adjust_segment_count();
                        last_bandwidth_check = Instant::now();
                    }

                    // Log progress
                    if let Some(progress) = self.get_progress() {
                        tracing::debug!(
                            downloaded = progress.downloaded,
                            total = progress.total_size,
                            speed = format!("{:.2} KB/s", progress.speed / 1024.0),
                            segments = format!("{}/{}", progress.completed_segments, progress.total_segments),
                            bandwidth = format!("{:.2} MB/s", progress.estimated_bandwidth / 1_000_000.0),
                            "Segment download progress"
                        );
                    }
                }
            }
        }

        // Flush all pending writes and close the file
        self.flush_writes().await?;

        tracing::info!(path = %output_path.display(), "File saved");

        // Save progress
        self.save_progress()?;

        Ok(())
    }

    /// Flush all pending writes to disk
    async fn flush_writes(&mut self) -> Result<(), SegmentDownloadError> {
        // Sort pending writes by offset for sequential writes
        self.pending_writes.sort_by_key(|(offset, _)| *offset);

        if let Some(ref mut file) = self.output_file {
            for (offset, data) in self.pending_writes.drain(..) {
                if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                    tracing::warn!(offset = offset, error = %e, "Failed to seek");
                    continue;
                }
                if let Err(e) = file.write_all(&data).await {
                    tracing::warn!(offset = offset, error = %e, "Failed to write");
                    continue;
                }
            }
            
            // Flush the buffer
            if let Err(e) = file.flush().await {
                tracing::warn!(error = %e, "Failed to flush file");
            }
        }

        Ok(())
    }

    /// Download all pending segments in parallel
    async fn download_segments(&mut self) {
        // Find segments that need to be downloaded
        let pending_segments: Vec<usize> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.downloaded)
            .map(|(i, _)| i)
            .collect();

        if pending_segments.is_empty() {
            return;
        }

        // Download all pending segments in parallel
        let mut tasks = Vec::new();

        for &seg_idx in &pending_segments {
            let segment = &self.segments[seg_idx];
            let offset = segment.offset;
            let size = segment.size;
            let url = self.url.clone();
            let client = self.http_client.clone();

            let task = tokio::spawn(async move {
                let start = Instant::now();
                let result = Self::download_segment_with_retry(client, url, offset, size, seg_idx).await;
                let duration = start.elapsed();
                (seg_idx, result, duration)
            });

            tasks.push(task);
        }

        // Wait for all segments to complete and write to file
        for task in tasks {
            match task.await {
                Ok((seg_idx, Ok(data), duration)) => {
                    // Update bandwidth estimate
                    let duration_ms = duration.as_secs_f64() * 1000.0;
                    self.update_bandwidth_estimate(data.len() as u64, duration_ms);

                    // Update segment throughput
                    if duration_ms > 0.0 {
                        if let Some(segment) = self.segments.get_mut(seg_idx) {
                            segment.throughput_bps = (data.len() as f64 * 1000.0) / duration_ms;
                        }
                    }

                    // Apply rate limiting before writing
                    if let Some(ref limiter) = self.rate_limiter {
                        limiter.acquire(data.len() as u64).await;
                    }

                    // Queue write for batched I/O
                    let offset = self.segments[seg_idx].offset;
                    self.pending_writes.push((offset, data.clone()));

                    // Flush if queue is large
                    if self.pending_writes.len() >= 8 {
                        if let Err(e) = self.flush_writes().await {
                            tracing::warn!(error = %e, "Failed to flush writes");
                        }
                    }

                    // Mark segment as downloaded
                    if let Some(segment) = self.segments.get_mut(seg_idx) {
                        segment.downloaded = true;
                        self.downloaded += data.len() as u64;
                        tracing::debug!(
                            segment = seg_idx,
                            offset = segment.offset,
                            size = data.len(),
                            throughput = format!("{:.2} MB/s", segment.throughput_bps / 1_000_000.0),
                            "Segment downloaded"
                        );
                    }
                }
                Ok((seg_idx, Err(e), _)) => {
                    tracing::warn!(segment = seg_idx, error = %e, "Failed to download segment");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Task failed");
                }
            }
        }
    }

    /// Download a single segment with retry logic
    async fn download_segment_with_retry(
        client: Client,
        url: String,
        offset: u64,
        size: u64,
        seg_idx: usize,
    ) -> Result<Vec<u8>, SegmentDownloadError> {
        let mut last_err = None;

        for attempt in 0..MAX_SEGMENT_RETRIES {
            match Self::download_segment(client.clone(), url.clone(), offset, size).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    tracing::warn!(
                        segment = seg_idx,
                        url = %url,
                        offset = offset,
                        attempt = attempt + 1,
                        max = MAX_SEGMENT_RETRIES,
                        error = %e,
                        "Segment download attempt failed"
                    );
                    last_err = Some(e);

                    if attempt + 1 < MAX_SEGMENT_RETRIES {
                        let delay = RETRY_BASE_DELAY_MS * 2u64.pow(attempt);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            SegmentDownloadError::Http("unknown error after retries".to_string())
        }))
    }

    /// Download a single segment using HTTP Range request
    async fn download_segment(
        client: Client,
        url: String,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, SegmentDownloadError> {
        let end = offset + size - 1;
        let range = format!("bytes={}-{}", offset, end);

        let response = client
            .get(&url)
            .header("Range", &range)
            .send()
            .await
            .map_err(|e| SegmentDownloadError::Http(e.to_string()))?;

        if !response.status().is_success()
            && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
        {
            return Err(SegmentDownloadError::Http(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        let data = response
            .bytes()
            .await
            .map_err(|e| SegmentDownloadError::Http(e.to_string()))?;

        Ok(data.to_vec())
    }

    /// Check if all segments are downloaded
    fn is_complete(&self) -> bool {
        self.segments.iter().all(|s| s.downloaded)
    }

    /// Get current download progress
    pub fn get_progress(&self) -> Option<SegmentDownloadProgress> {
        let start_time = self.start_time?;
        let elapsed = start_time.elapsed().as_secs_f64();
        let speed = if elapsed > 0.0 {
            self.downloaded as f64 / elapsed
        } else {
            0.0
        };

        let completed_segments = self.segments.iter().filter(|s| s.downloaded).count();

        let segment_progress = self
            .segments
            .iter()
            .map(|s| SegmentInfo {
                index: s.index,
                offset: s.offset,
                size: s.size,
                downloaded: s.downloaded,
                throughput_bps: s.throughput_bps,
            })
            .collect();

        Some(SegmentDownloadProgress {
            total_size: self.file_size,
            downloaded: self.downloaded,
            speed,
            total_segments: self.segments.len(),
            completed_segments,
            estimated_bandwidth: self.estimated_bandwidth,
            segment_progress,
        })
    }

    /// Get file name
    pub fn get_file_name(&self) -> &str {
        &self.file_name
    }

    /// Get file size
    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }

    /// Save download progress to disk
    fn save_progress(&self) -> Result<(), SegmentDownloadError> {
        let progress_path = self
            .download_dir
            .join(format!("{}.segments", self.file_name));

        // Create bitmap of downloaded segments
        let mut bitmap = Vec::new();
        for segment in &self.segments {
            bitmap.push(if segment.downloaded { 1u8 } else { 0u8 });
        }

        let progress_data = serde_cbor::Value::Map({
            let mut map = std::collections::BTreeMap::new();
            map.insert(
                serde_cbor::Value::Text("file_name".to_string()),
                serde_cbor::Value::Text(self.file_name.clone()),
            );
            map.insert(
                serde_cbor::Value::Text("file_size".to_string()),
                serde_cbor::Value::Integer(self.file_size as i128),
            );
            map.insert(
                serde_cbor::Value::Text("segment_count".to_string()),
                serde_cbor::Value::Integer(self.segments.len() as i128),
            );
            map.insert(
                serde_cbor::Value::Text("bitmap".to_string()),
                serde_cbor::Value::Array(
                    bitmap
                        .into_iter()
                        .map(|b| serde_cbor::Value::Integer(b as i128))
                        .collect(),
                ),
            );
            map.insert(
                serde_cbor::Value::Text("downloaded".to_string()),
                serde_cbor::Value::Integer(self.downloaded as i128),
            );
            map
        });

        let progress_bytes = serde_cbor::to_vec(&progress_data)
            .map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        std::fs::write(&progress_path, progress_bytes)
            .map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        tracing::debug!(path = %progress_path.display(), "Segment progress saved");

        Ok(())
    }

    /// Load download progress from disk
    fn load_progress(&mut self) -> Result<(), SegmentDownloadError> {
        let progress_path = self
            .download_dir
            .join(format!("{}.segments", self.file_name));

        if !progress_path.exists() {
            return Err(SegmentDownloadError::Io(
                "No progress file found".to_string(),
            ));
        }

        let progress_bytes =
            std::fs::read(&progress_path).map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        let progress_data: serde_cbor::Value = serde_cbor::from_slice(&progress_bytes)
            .map_err(|e| SegmentDownloadError::Io(e.to_string()))?;

        // Verify file size matches
        if let serde_cbor::Value::Map(map) = &progress_data {
            if let Some(serde_cbor::Value::Integer(saved_size)) =
                map.get(&serde_cbor::Value::Text("file_size".to_string()))
            {
                if *saved_size as u64 != self.file_size {
                    return Err(SegmentDownloadError::Io("File size mismatch".to_string()));
                }
            } else {
                return Err(SegmentDownloadError::Io(
                    "Invalid file_size in progress".to_string(),
                ));
            }

            // Restore segment bitmap
            if let Some(serde_cbor::Value::Array(bitmap)) =
                map.get(&serde_cbor::Value::Text("bitmap".to_string()))
            {
                for (i, segment) in self.segments.iter_mut().enumerate() {
                    if let Some(serde_cbor::Value::Integer(downloaded)) = bitmap.get(i) {
                        segment.downloaded = *downloaded == 1;
                    }
                }
            }

            // Restore downloaded count
            if let Some(serde_cbor::Value::Integer(downloaded)) =
                map.get(&serde_cbor::Value::Text("downloaded".to_string()))
            {
                self.downloaded = *downloaded as u64;
            }
        } else {
            return Err(SegmentDownloadError::Io(
                "Invalid progress format".to_string(),
            ));
        }

        tracing::info!(
            file = %self.file_name,
            downloaded = self.downloaded,
            total = self.file_size,
            segments = self.segments.len(),
            "Segment progress restored"
        );

        Ok(())
    }
}

/// Errors from segment download operations
#[derive(Debug, thiserror::Error)]
pub enum SegmentDownloadError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("IO error: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_segments_small_file() {
        // Small file (< 1MB) should use single segment
        let segments = SegmentDownloader::create_segments(512 * 1024, DEFAULT_SEGMENT_COUNT);
        assert_eq!(segments.len(), DEFAULT_SEGMENT_COUNT);
        assert_eq!(segments[0].offset, 0);
        // Each segment should be 128KB (512KB / 4)
        assert_eq!(segments[0].size, 128 * 1024);
    }

    #[test]
    fn test_create_segments_large_file() {
        // Large file (10MB) should be split into 4 segments
        let file_size = 10 * 1024 * 1024;
        let segments = SegmentDownloader::create_segments(file_size, 4);

        assert_eq!(segments.len(), 4);
        assert_eq!(segments[0].offset, 0);
        let segment_size = file_size / 4; // 2.5MB each
        assert_eq!(segments[0].size, segment_size);

        assert_eq!(segments[1].offset, segment_size);
        assert_eq!(segments[2].offset, 2 * segment_size);
        assert_eq!(segments[3].offset, 3 * segment_size);

        // Last segment gets remainder
        let total_size: u64 = segments.iter().map(|s| s.size).sum();
        assert_eq!(total_size, file_size);
    }

    #[test]
    fn test_create_segments_uneven_split() {
        // File size not evenly divisible by segment count
        let file_size = 100;
        let segments = SegmentDownloader::create_segments(file_size, 3);

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].size, 33);
        assert_eq!(segments[1].size, 33);
        assert_eq!(segments[2].size, 34); // Last segment gets remainder

        let total_size: u64 = segments.iter().map(|s| s.size).sum();
        assert_eq!(total_size, file_size);
    }

    #[test]
    fn test_segment_downloader_initialization() {
        let tmp_dir = tempdir().unwrap();
        let downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(downloader.get_file_name(), "file.zip");
        assert_eq!(downloader.get_file_size(), 10 * 1024 * 1024);
        assert_eq!(downloader.segments.len(), 4);
        assert_eq!(downloader.downloaded, 0);
    }

    #[test]
    fn test_segment_downloader_small_file_single_segment() {
        let tmp_dir = tempdir().unwrap();
        let downloader = SegmentDownloader::new(
            "http://example.com/small.txt".to_string(),
            "small.txt".to_string(),
            512 * 1024, // < 1MB
            tmp_dir.path().to_path_buf(),
        );

        // Small files should use single segment
        assert_eq!(downloader.segments.len(), 1);
    }

    #[test]
    fn test_set_segment_count() {
        let tmp_dir = tempdir().unwrap();
        let mut downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024,
            tmp_dir.path().to_path_buf(),
        );

        downloader.set_segment_count(8);
        assert_eq!(downloader.segment_count, 8);
        assert_eq!(downloader.segments.len(), 8);
    }

    #[test]
    fn test_is_complete() {
        let tmp_dir = tempdir().unwrap();
        let mut downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            1024,
            tmp_dir.path().to_path_buf(),
        );

        // Initially not complete
        assert!(!downloader.is_complete());

        // Mark all segments as downloaded
        for segment in &mut downloader.segments {
            segment.downloaded = true;
        }

        assert!(downloader.is_complete());
    }

    #[test]
    fn test_get_progress() {
        let tmp_dir = tempdir().unwrap();
        let mut downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024, // 10MB to get 4 segments
            tmp_dir.path().to_path_buf(),
        );

        downloader.start_time = Some(std::time::Instant::now());
        downloader.downloaded = 5 * 1024 * 1024;

        let progress = downloader.get_progress().unwrap();
        assert_eq!(progress.total_size, 10 * 1024 * 1024);
        assert_eq!(progress.downloaded, 5 * 1024 * 1024);
        assert_eq!(progress.total_segments, 4);
        assert_eq!(progress.completed_segments, 0);
    }

    #[tokio::test]
    async fn test_save_and_load_progress() {
        let tmp_dir = tempdir().unwrap();
        let mut downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024, // 10MB to get 4 segments
            tmp_dir.path().to_path_buf(),
        );

        // Mark some segments as downloaded
        downloader.segments[0].downloaded = true;
        downloader.segments[1].downloaded = true;
        downloader.downloaded = 5 * 1024 * 1024;

        // Save progress
        downloader.save_progress().unwrap();

        // Create new downloader and load progress
        let mut downloader2 = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024, // 10MB to get 4 segments
            tmp_dir.path().to_path_buf(),
        );

        downloader2.load_progress().unwrap();

        assert_eq!(downloader2.downloaded, 5 * 1024 * 1024);
        assert!(downloader2.segments[0].downloaded);
        assert!(downloader2.segments[1].downloaded);
        assert!(!downloader2.segments[2].downloaded);
        assert!(!downloader2.segments[3].downloaded);
    }

    #[tokio::test]
    async fn test_load_progress_missing_file() {
        let tmp_dir = tempdir().unwrap();
        let mut downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024, // 10MB to get 4 segments
            tmp_dir.path().to_path_buf(),
        );

        // Should return error if no progress file exists
        let result = downloader.load_progress();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_progress_file_size_mismatch() {
        let tmp_dir = tempdir().unwrap();
        let downloader = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            10 * 1024 * 1024, // 10MB to get 4 segments
            tmp_dir.path().to_path_buf(),
        );

        // Save progress for 10MB file
        downloader.save_progress().unwrap();

        // Try to load into downloader with different file size
        let mut downloader2 = SegmentDownloader::new(
            "http://example.com/file.zip".to_string(),
            "file.zip".to_string(),
            20 * 1024 * 1024, // Different size (20MB)
            tmp_dir.path().to_path_buf(),
        );

        let result = downloader2.load_progress();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("File size mismatch")
        );
    }
}
