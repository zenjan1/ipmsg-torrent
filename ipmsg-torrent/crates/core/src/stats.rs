//! Network statistics and metrics tracking
//! Inspired by libp2p metrics and qaul.net monitoring

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Network statistics tracker
pub struct NetworkStats {
    /// Total messages sent
    pub messages_sent: AtomicU64,
    /// Total messages received
    pub messages_received: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Current number of connected peers
    pub peers_connected: AtomicU64,
    /// Total peer connections made
    pub total_connections: AtomicU64,
    /// Total peer disconnections
    pub total_disconnections: AtomicU64,
    /// Total files shared
    pub files_shared: AtomicU64,
    /// Total files downloaded
    pub files_downloaded: AtomicU64,
    /// Total bytes uploaded for files
    pub file_bytes_uploaded: AtomicU64,
    /// Total bytes downloaded for files
    pub file_bytes_downloaded: AtomicU64,
    /// Total duplicate messages rejected
    pub duplicates_rejected: AtomicU64,
    /// Total invalid messages rejected
    pub invalid_rejected: AtomicU64,
    /// Total messages from blocked peers
    pub blocked_messages: AtomicU64,
    /// Total message processing latency in microseconds (for avg calculation)
    pub processing_latency_us: AtomicU64,
    /// Number of messages processed (for avg latency)
    pub processing_count: AtomicU64,
    /// Peak message processing latency in microseconds
    pub peak_processing_latency_us: AtomicU64,
    /// Total serialization operations
    pub serialization_count: AtomicU64,
    /// Total serialization time in microseconds
    pub serialization_time_us: AtomicU64,
    /// Total deserialization operations
    pub deserialization_count: AtomicU64,
    /// Total deserialization time in microseconds
    pub deserialization_time_us: AtomicU64,
    /// Total gossipsub messages propagated
    pub gossipsub_propagated: AtomicU64,
    /// Total gossipsub messages received
    pub gossipsub_received: AtomicU64,
    /// Start time for uptime calculation
    start_time: Instant,
}

impl NetworkStats {
    /// Create a new stats tracker
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            peers_connected: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            total_disconnections: AtomicU64::new(0),
            files_shared: AtomicU64::new(0),
            files_downloaded: AtomicU64::new(0),
            file_bytes_uploaded: AtomicU64::new(0),
            file_bytes_downloaded: AtomicU64::new(0),
            duplicates_rejected: AtomicU64::new(0),
            invalid_rejected: AtomicU64::new(0),
            blocked_messages: AtomicU64::new(0),
            processing_latency_us: AtomicU64::new(0),
            processing_count: AtomicU64::new(0),
            peak_processing_latency_us: AtomicU64::new(0),
            serialization_count: AtomicU64::new(0),
            serialization_time_us: AtomicU64::new(0),
            deserialization_count: AtomicU64::new(0),
            deserialization_time_us: AtomicU64::new(0),
            gossipsub_propagated: AtomicU64::new(0),
            gossipsub_received: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Record a message sent
    pub fn record_message_sent(&self, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a message received
    pub fn record_message_received(&self, bytes: u64) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a peer connection
    pub fn record_peer_connected(&self) {
        self.peers_connected.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a peer disconnection
    pub fn record_peer_disconnected(&self) {
        self.peers_connected.fetch_sub(1, Ordering::Relaxed);
        self.total_disconnections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a file shared
    pub fn record_file_shared(&self, bytes: u64) {
        self.files_shared.fetch_add(1, Ordering::Relaxed);
        self.file_bytes_uploaded.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a file downloaded
    pub fn record_file_downloaded(&self, bytes: u64) {
        self.files_downloaded.fetch_add(1, Ordering::Relaxed);
        self.file_bytes_downloaded
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a duplicate message rejected
    pub fn record_duplicate_rejected(&self) {
        self.duplicates_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an invalid message rejected
    pub fn record_invalid_rejected(&self) {
        self.invalid_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a message from blocked peer
    pub fn record_blocked_message(&self) {
        self.blocked_messages.fetch_add(1, Ordering::Relaxed);
    }

    /// Record message processing latency (microseconds).
    /// Updates running total, count, and peak for average/peak calculations.
    #[inline]
    pub fn record_processing_latency(&self, latency_us: u64) {
        self.processing_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.processing_count.fetch_add(1, Ordering::Relaxed);
        // Update peak if this is the new max (lock-free CAS loop)
        let mut current_peak = self.peak_processing_latency_us.load(Ordering::Relaxed);
        while latency_us > current_peak {
            match self.peak_processing_latency_us.compare_exchange_weak(
                current_peak,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(old) => current_peak = old,
            }
        }
    }

    /// Record serialization operation (for performance monitoring)
    #[inline]
    pub fn record_serialization(&self, time_us: u64) {
        self.serialization_count.fetch_add(1, Ordering::Relaxed);
        self.serialization_time_us.fetch_add(time_us, Ordering::Relaxed);
    }

    /// Record deserialization operation (for performance monitoring)
    #[inline]
    pub fn record_deserialization(&self, time_us: u64) {
        self.deserialization_count.fetch_add(1, Ordering::Relaxed);
        self.deserialization_time_us.fetch_add(time_us, Ordering::Relaxed);
    }

    /// Record gossipsub message propagation
    #[inline]
    pub fn record_gossipsub_propagated(&self) {
        self.gossipsub_propagated.fetch_add(1, Ordering::Relaxed);
    }

    /// Record gossipsub message received
    #[inline]
    pub fn record_gossipsub_received(&self) {
        self.gossipsub_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Get average serialization time in microseconds
    pub fn avg_serialization_time_us(&self) -> f64 {
        let count = self.serialization_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let total = self.serialization_time_us.load(Ordering::Relaxed);
        total as f64 / count as f64
    }

    /// Get average deserialization time in microseconds
    pub fn avg_deserialization_time_us(&self) -> f64 {
        let count = self.deserialization_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let total = self.deserialization_time_us.load(Ordering::Relaxed);
        total as f64 / count as f64
    }

    /// Get gossipsub propagation rate (propagated / received)
    pub fn gossipsub_propagation_rate(&self) -> f64 {
        let received = self.gossipsub_received.load(Ordering::Relaxed);
        if received == 0 {
            return 0.0;
        }
        let propagated = self.gossipsub_propagated.load(Ordering::Relaxed);
        propagated as f64 / received as f64
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Get uptime as human-readable string
    pub fn uptime_string(&self) -> String {
        let secs = self.uptime_seconds();
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        let mins = (secs % 3600) / 60;
        let secs = secs % 60;

        if days > 0 {
            format!("{}d {}h {}m {}s", days, hours, mins, secs)
        } else if hours > 0 {
            format!("{}h {}m {}s", hours, mins, secs)
        } else if mins > 0 {
            format!("{}m {}s", mins, secs)
        } else {
            format!("{}s", secs)
        }
    }

    /// Get current connected peers count
    pub fn connected_peers(&self) -> u64 {
        self.peers_connected.load(Ordering::Relaxed)
    }

    /// Get messages per second (average)
    pub fn messages_per_second(&self) -> f64 {
        let uptime = self.uptime_seconds() as f64;
        if uptime == 0.0 {
            return 0.0;
        }
        let total = self.messages_sent.load(Ordering::Relaxed)
            + self.messages_received.load(Ordering::Relaxed);
        total as f64 / uptime
    }

    /// Get bytes per second (average)
    pub fn bytes_per_second(&self) -> f64 {
        let uptime = self.uptime_seconds() as f64;
        if uptime == 0.0 {
            return 0.0;
        }
        let total =
            self.bytes_sent.load(Ordering::Relaxed) + self.bytes_received.load(Ordering::Relaxed);
        total as f64 / uptime
    }

    /// Average message processing latency in microseconds
    pub fn avg_processing_latency_us(&self) -> f64 {
        let count = self.processing_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let total = self.processing_latency_us.load(Ordering::Relaxed);
        total as f64 / count as f64
    }

    /// Peak message processing latency in microseconds
    pub fn peak_processing_latency_us(&self) -> u64 {
        self.peak_processing_latency_us.load(Ordering::Relaxed)
    }

    /// Format stats as human-readable string
    pub fn summary(&self) -> String {
        let msg_rate = format!("{:.2}", self.messages_per_second());
        let bandwidth = format!("{:.2}", self.bytes_per_second());
        let avg_lat = self.avg_processing_latency_us();
        let peak_lat = self.peak_processing_latency_us();
        let avg_ser = self.avg_serialization_time_us();
        let avg_deser = self.avg_deserialization_time_us();
        let gossip_rate = self.gossipsub_propagation_rate();
        format!(
            "Uptime: {}\n\
             Peers: {} connected ({} total connections)\n\
             Messages: {} sent, {} received ({} msg/s avg)\n\
             Traffic: {} sent, {} received ({} B/s avg)\n\
             Files: {} shared, {} downloaded\n\
             Rejected: {} duplicates, {} invalid, {} blocked\n\
             Latency: avg {:.1} µs, peak {} µs\n\
             Serialization: avg {:.1} µs\n\
             Deserialization: avg {:.1} µs\n\
             Gossipsub: {:.2}% propagation rate",
            self.uptime_string(),
            self.connected_peers(),
            self.total_connections.load(Ordering::Relaxed),
            self.messages_sent.load(Ordering::Relaxed),
            self.messages_received.load(Ordering::Relaxed),
            msg_rate,
            format_bytes(self.bytes_sent.load(Ordering::Relaxed)),
            format_bytes(self.bytes_received.load(Ordering::Relaxed)),
            bandwidth,
            self.files_shared.load(Ordering::Relaxed),
            self.files_downloaded.load(Ordering::Relaxed),
            self.duplicates_rejected.load(Ordering::Relaxed),
            self.invalid_rejected.load(Ordering::Relaxed),
            self.blocked_messages.load(Ordering::Relaxed),
            avg_lat,
            peak_lat,
            avg_ser,
            avg_deser,
            gossip_rate * 100.0,
        )
    }
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Format bytes into human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_stats_new() {
        let stats = NetworkStats::new();
        assert_eq!(stats.connected_peers(), 0);
        assert_eq!(stats.uptime_seconds(), 0);
    }

    #[test]
    fn test_record_message() {
        let stats = NetworkStats::new();
        stats.record_message_sent(100);
        stats.record_message_received(200);

        assert_eq!(stats.messages_sent.load(Ordering::Relaxed), 1);
        assert_eq!(stats.messages_received.load(Ordering::Relaxed), 1);
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 100);
        assert_eq!(stats.bytes_received.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn test_record_peer() {
        let stats = NetworkStats::new();
        stats.record_peer_connected();
        stats.record_peer_connected();

        assert_eq!(stats.connected_peers(), 2);
        assert_eq!(stats.total_connections.load(Ordering::Relaxed), 2);

        stats.record_peer_disconnected();
        assert_eq!(stats.connected_peers(), 1);
        assert_eq!(stats.total_disconnections.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_uptime_string() {
        let stats = NetworkStats::new();
        // Just created, should be 0s
        assert_eq!(stats.uptime_string(), "0s");
    }

    #[test]
    fn test_summary() {
        let stats = NetworkStats::new();
        stats.record_message_sent(100);
        stats.record_peer_connected();
        let summary = stats.summary();
        assert!(summary.contains("Uptime:"));
        assert!(summary.contains("Peers:"));
        assert!(summary.contains("Messages:"));
    }

    #[test]
    fn test_processing_latency() {
        let stats = NetworkStats::new();
        stats.record_processing_latency(100);
        stats.record_processing_latency(200);
        stats.record_processing_latency(50);

        assert_eq!(stats.processing_count.load(Ordering::Relaxed), 3);
        assert_eq!(stats.processing_latency_us.load(Ordering::Relaxed), 350);
        assert_eq!(stats.avg_processing_latency_us(), 350.0 / 3.0);
        assert_eq!(stats.peak_processing_latency_us(), 200);
    }

    #[test]
    fn test_processing_latency_peak_update() {
        let stats = NetworkStats::new();
        stats.record_processing_latency(500);
        assert_eq!(stats.peak_processing_latency_us(), 500);
        // Lower value should not update peak
        stats.record_processing_latency(100);
        assert_eq!(stats.peak_processing_latency_us(), 500);
        // Higher value should update peak
        stats.record_processing_latency(1000);
        assert_eq!(stats.peak_processing_latency_us(), 1000);
    }
}
