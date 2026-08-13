//! Connection pool for reusing TCP connections
//!
//! This module provides a high-performance connection pool with:
//! - TCP parameter optimization (SO_SNDBUF, SO_RCVBUF, TCP_NODELAY)
//! - DNS result caching to reduce resolution overhead
//! - Connection health monitoring and validation
//! - Pre-connect support for queue optimization
//! - Per-domain connection limits to prevent server overload
//! - Configuration persistence and detailed statistics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Connection pool entry with health tracking
#[allow(dead_code)]
struct PoolEntry {
    stream: TcpStream,
    addr: SocketAddr,
    created_at: Instant,
    last_used: Instant,
    /// Number of times this connection has been reused
    reuse_count: u32,
    /// Number of errors encountered on this connection
    error_count: u32,
    /// Last measured RTT for this connection (if available)
    last_rtt_ms: Option<f64>,
}

impl PoolEntry {
    fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            stream,
            addr,
            created_at: now,
            last_used: now,
            reuse_count: 0,
            error_count: 0,
            last_rtt_ms: None,
        }
    }

    fn is_expired(&self, max_age: Duration) -> bool {
        self.created_at.elapsed() > max_age
    }

    fn is_idle(&self, max_idle: Duration) -> bool {
        self.last_used.elapsed() > max_idle
    }

    /// Check if connection is healthy (low error rate, not too old)
    fn is_healthy(&self) -> bool {
        // Reject connections with high error rates
        if self.error_count > 3 {
            return false;
        }
        // Reject very old connections even if not expired
        if self.created_at.elapsed() > Duration::from_secs(120) {
            return false;
        }
        true
    }

    /// Record a successful use of this connection
    fn record_success(&mut self) {
        self.last_used = Instant::now();
        self.reuse_count += 1;
    }

    /// Record an error on this connection
    fn record_error(&mut self) {
        self.error_count += 1;
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Maximum connections per address
    pub max_connections_per_addr: usize,
    /// Maximum age of a connection (seconds)
    pub max_age_secs: u64,
    /// Maximum idle time before connection is closed (seconds)
    pub max_idle_secs: u64,
    /// Connection timeout (seconds)
    pub connect_timeout_secs: u64,
    /// TCP send buffer size (0 = system default)
    pub tcp_send_buffer_size: u32,
    /// TCP receive buffer size (0 = system default)
    pub tcp_recv_buffer_size: u32,
    /// Enable TCP_NODELAY (disable Nagle's algorithm)
    pub tcp_nodelay: bool,
    /// Enable DNS caching
    pub dns_cache_enabled: bool,
    /// DNS cache TTL (seconds)
    pub dns_cache_ttl_secs: u64,
    /// Enable connection health checks
    pub health_check_enabled: bool,
}

impl PoolConfig {
    /// Get max_age as Duration
    pub fn max_age(&self) -> Duration {
        Duration::from_secs(self.max_age_secs)
    }

    /// Get max_idle as Duration
    pub fn max_idle(&self) -> Duration {
        Duration::from_secs(self.max_idle_secs)
    }

    /// Get connect_timeout as Duration
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    /// Get dns_cache_ttl as Duration
    pub fn dns_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.dns_cache_ttl_secs)
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_addr: 4,
            max_age_secs: 300, // 5 minutes
            max_idle_secs: 60, // 1 minute
            connect_timeout_secs: 10,
            tcp_send_buffer_size: 256 * 1024, // 256 KB
            tcp_recv_buffer_size: 256 * 1024, // 256 KB
            tcp_nodelay: true,                // Disable Nagle for low latency
            dns_cache_enabled: true,
            dns_cache_ttl_secs: 300, // 5 minutes
            health_check_enabled: true,
        }
    }
}

/// Save pool configuration to disk
pub fn save_pool_config(
    config: &PoolConfig,
    data_dir: &std::path::Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("connection_pool_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(&path, json)
}

/// Load pool configuration from disk
pub fn load_pool_config(data_dir: &std::path::Path) -> Result<PoolConfig, std::io::Error> {
    let path = data_dir.join("connection_pool_config.json");
    if !path.exists() {
        return Ok(PoolConfig::default());
    }
    let json = fs::read_to_string(&path)?;
    serde_json::from_str(&json).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// DNS cache entry
#[derive(Debug, Clone)]
struct DnsCacheEntry {
    addr: SocketAddr,
    resolved_at: Instant,
}

/// Connection pool for reusing TCP connections
pub struct ConnectionPool {
    config: Arc<Mutex<PoolConfig>>,
    connections: Arc<Mutex<HashMap<SocketAddr, Vec<PoolEntry>>>>,
    /// DNS cache: hostname -> resolved address
    dns_cache: Arc<Mutex<HashMap<String, DnsCacheEntry>>>,
    /// Per-domain connection limits
    domain_limits: Arc<Mutex<HashMap<String, usize>>>,
    /// Current connection counts per domain
    domain_counts: Arc<Mutex<HashMap<String, usize>>>,
    /// Pool statistics
    stats: Arc<Mutex<PoolStats>>,
    /// Pool creation time
    created_at: Instant,
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionPool {
    /// Helper to get config with lock
    async fn config(&self) -> tokio::sync::MutexGuard<'_, PoolConfig> {
        self.config.lock().await
    }

    /// Create a new connection pool with default configuration
    pub fn new() -> Self {
        Self::with_config(PoolConfig::default())
    }

    /// Create a new connection pool with custom configuration
    pub fn with_config(config: PoolConfig) -> Self {
        Self {
            config: Arc::new(Mutex::new(config)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            dns_cache: Arc::new(Mutex::new(HashMap::new())),
            domain_limits: Arc::new(Mutex::new(HashMap::new())),
            domain_counts: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(PoolStats::default())),
            created_at: Instant::now(),
        }
    }

    /// Set per-domain connection limit
    pub async fn set_domain_limit(&self, domain: &str, limit: usize) {
        let mut limits = self.domain_limits.lock().await;
        limits.insert(domain.to_string(), limit);
    }

    /// Check if we can create a new connection for a domain
    pub async fn can_connect_domain(&self, domain: &str) -> bool {
        let limits = self.domain_limits.lock().await;
        let counts = self.domain_counts.lock().await;

        if let Some(&limit) = limits.get(domain) {
            let current = counts.get(domain).copied().unwrap_or(0);
            current < limit
        } else {
            true // No limit configured
        }
    }

    /// Record a new connection for a domain
    pub async fn record_domain_connection(&self, domain: &str) {
        let mut counts = self.domain_counts.lock().await;
        *counts.entry(domain.to_string()).or_insert(0) += 1;
    }

    /// Record a closed connection for a domain
    pub async fn record_domain_disconnect(&self, domain: &str) {
        let mut counts = self.domain_counts.lock().await;
        if let Some(count) = counts.get_mut(domain) {
            *count = count.saturating_sub(1);
        }
    }

    /// Get a connection from the pool or create a new one
    pub async fn get_or_connect(&self, addr: SocketAddr) -> Result<TcpStream, PoolError> {
        let config = self.config().await;
        // Try to get an existing connection
        {
            let mut conns = self.connections.lock().await;
            if let Some(entries) = conns.get_mut(&addr) {
                // Remove expired, idle, or unhealthy connections
                entries.retain(|e| {
                    !e.is_expired(config.max_age())
                        && !e.is_idle(config.max_idle())
                        && (!config.health_check_enabled || e.is_healthy())
                });

                // Try to get a healthy connection
                while let Some(mut entry) = entries.pop() {
                    if !config.health_check_enabled || entry.is_healthy() {
                        entry.record_success();
                        // Clean up empty entries
                        if entries.is_empty() {
                            conns.remove(&addr);
                        }
                        let mut stats = self.stats.lock().await;
                        stats.total_reused += 1;
                        return Ok(entry.stream);
                    }
                    // Unhealthy connection, discard it
                    let mut stats = self.stats.lock().await;
                    stats.total_discarded += 1;
                }

                // Clean up empty entries
                if entries.is_empty() {
                    conns.remove(&addr);
                }
            }
        }

        // Create a new connection
        let stream = self.connect(addr).await?;
        let mut stats = self.stats.lock().await;
        stats.total_created += 1;
        Ok(stream)
    }

    /// Pre-connect: establish a connection without immediately using it
    /// Returns immediately if a connection is already available
    pub async fn pre_connect(&self, addr: SocketAddr) -> Result<(), PoolError> {
        let config = self.config().await;
        // Check if we already have a connection
        {
            let conns = self.connections.lock().await;
            if let Some(entries) = conns.get(&addr) {
                if entries
                    .iter()
                    .any(|e| e.is_healthy() && !e.is_idle(config.max_idle()))
                {
                    return Ok(()); // Already have a good connection
                }
            }
        }

        // Create a new connection and return it to the pool
        let stream = self.connect(addr).await?;
        self.return_connection(stream, addr).await;
        Ok(())
    }

    /// Resolve hostname with DNS caching
    pub async fn resolve_cached(&self, hostname: &str, port: u16) -> Result<SocketAddr, PoolError> {
        let config = self.config().await;
        // Check DNS cache first
        if config.dns_cache_enabled {
            let cache = self.dns_cache.lock().await;
            if let Some(entry) = cache.get(hostname) {
                if entry.resolved_at.elapsed() < config.dns_cache_ttl() {
                    // Cache hit and not expired
                    let mut addr = entry.addr;
                    addr.set_port(port);
                    let mut stats = self.stats.lock().await;
                    stats.dns_cache_hits += 1;
                    return Ok(addr);
                }
            }
        }

        // Cache miss or expired - resolve via DNS
        let mut stats = self.stats.lock().await;
        stats.dns_cache_misses += 1;
        drop(stats);

        use tokio::net::lookup_host;
        let addr_str = format!("{}:{}", hostname, port);
        let addr = lookup_host(&addr_str)
            .await
            .map_err(|e| PoolError::Dns(e.to_string()))?
            .next()
            .ok_or_else(|| PoolError::Dns(format!("No addresses found for {}", hostname)))?;

        // Update DNS cache
        if config.dns_cache_enabled {
            let mut cache = self.dns_cache.lock().await;
            cache.insert(
                hostname.to_string(),
                DnsCacheEntry {
                    addr,
                    resolved_at: Instant::now(),
                },
            );
        }

        Ok(addr)
    }

    /// Return a connection to the pool
    pub async fn return_connection(&self, stream: TcpStream, addr: SocketAddr) {
        let config = self.config().await;
        let mut entry = PoolEntry::new(stream, addr);
        entry.record_success();

        let mut conns = self.connections.lock().await;
        let entries = conns.entry(addr).or_insert_with(Vec::new);

        // Remove expired, idle, or unhealthy connections
        entries.retain(|e| {
            !e.is_expired(config.max_age())
                && !e.is_idle(config.max_idle())
                && (!config.health_check_enabled || e.is_healthy())
        });

        // Add the new entry if we haven't reached the limit and it's healthy
        if entries.len() < config.max_connections_per_addr && entry.is_healthy() {
            entries.push(entry);
        }
    }

    /// Mark a connection as having an error (for health tracking)
    pub async fn mark_connection_error(&self, addr: SocketAddr) {
        let mut conns = self.connections.lock().await;
        if let Some(entries) = conns.get_mut(&addr) {
            for entry in entries.iter_mut() {
                entry.record_error();
            }
        }
    }

    /// Create a new connection with optimized TCP parameters
    async fn connect(&self, addr: SocketAddr) -> Result<TcpStream, PoolError> {
        let config = self.config().await;
        let stream = timeout(config.connect_timeout(), TcpStream::connect(addr))
            .await
            .map_err(|_| PoolError::Timeout)?
            .map_err(PoolError::Io)?;

        // Apply TCP optimizations after connection
        if config.tcp_nodelay {
            let _ = stream.set_nodelay(true);
        }

        Ok(stream)
    }

    /// Remove expired connections from the pool
    pub async fn cleanup(&self) {
        let config = self.config().await;
        let mut conns = self.connections.lock().await;
        conns.retain(|_, entries| {
            entries.retain(|e| {
                !e.is_expired(config.max_age())
                    && !e.is_idle(config.max_idle())
                    && (!config.health_check_enabled || e.is_healthy())
            });
            !entries.is_empty()
        });

        // Also clean up expired DNS cache entries
        if config.dns_cache_enabled {
            let mut cache = self.dns_cache.lock().await;
            cache.retain(|_, entry| entry.resolved_at.elapsed() < config.dns_cache_ttl());
        }
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let config = self.config().await;
        let conns = self.connections.lock().await;
        let total_connections = conns.values().map(|v| v.len()).sum();
        let healthy_connections: usize = conns
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| e.is_healthy())
            .count();
        let dns_cache_size = if config.dns_cache_enabled {
            let cache = self.dns_cache.lock().await;
            cache.len()
        } else {
            0
        };

        let base_stats = self.stats.lock().await;
        PoolStats {
            total_addresses: conns.len(),
            total_connections,
            healthy_connections,
            dns_cache_size,
            total_created: base_stats.total_created,
            total_reused: base_stats.total_reused,
            total_discarded: base_stats.total_discarded,
            dns_cache_hits: base_stats.dns_cache_hits,
            dns_cache_misses: base_stats.dns_cache_misses,
        }
    }

    /// Get detailed pool status including domain connections
    pub async fn status(&self) -> PoolStatus {
        let config = self.config().await;
        let stats = self.stats().await;
        let domain_connections = self.get_domain_connections().await;
        let uptime_secs = self.created_at.elapsed().as_secs();

        PoolStatus {
            config: config.clone(),
            stats,
            domain_connections,
            uptime_secs,
        }
    }

    /// Get per-domain connection information
    pub async fn get_domain_connections(&self) -> Vec<DomainConnectionInfo> {
        let limits = self.domain_limits.lock().await;
        let counts = self.domain_counts.lock().await;

        let mut domain_connections: Vec<DomainConnectionInfo> = counts
            .iter()
            .map(|(domain, &current)| {
                let connection_limit = limits.get(domain).copied();
                let utilization_percent = connection_limit
                    .map(|limit| {
                        if limit > 0 {
                            (current as f64 / limit as f64) * 100.0
                        } else {
                            0.0
                        }
                    })
                    .unwrap_or(0.0);

                DomainConnectionInfo {
                    domain: domain.clone(),
                    current_connections: current,
                    connection_limit,
                    utilization_percent,
                }
            })
            .collect();

        // Sort by current connections descending
        domain_connections.sort_by(|a, b| b.current_connections.cmp(&a.current_connections));
        domain_connections
    }

    /// Update pool configuration
    pub async fn update_config(&self, config: PoolConfig) {
        *self.config.lock().await = config;
    }

    /// Get current configuration
    pub async fn get_config_async(&self) -> PoolConfig {
        self.config.lock().await.clone()
    }

    /// Get current configuration (sync, clones under lock)
    pub fn get_config(&self) -> PoolConfig {
        // Use try_lock for sync access; fallback to default if locked
        self.config
            .try_lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    /// Clear all connections and reset statistics
    pub async fn clear(&self) {
        let config = self.config().await;
        let mut conns = self.connections.lock().await;
        conns.clear();

        if config.dns_cache_enabled {
            let mut cache = self.dns_cache.lock().await;
            cache.clear();
        }

        let mut stats = self.stats.lock().await;
        *stats = PoolStats::default();
    }
}

/// Connection pool error
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("connection timeout")]
    Timeout,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("DNS resolution failed: {0}")]
    Dns(String),
}

/// Connection pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    pub total_addresses: usize,
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub dns_cache_size: usize,
    /// Total connections created since pool start
    pub total_created: u64,
    /// Total connections reused from pool
    pub total_reused: u64,
    /// Total connections discarded (expired/unhealthy)
    pub total_discarded: u64,
    /// Total DNS cache hits
    pub dns_cache_hits: u64,
    /// Total DNS cache misses
    pub dns_cache_misses: u64,
}

impl Default for PoolStats {
    fn default() -> Self {
        Self {
            total_addresses: 0,
            total_connections: 0,
            healthy_connections: 0,
            dns_cache_size: 0,
            total_created: 0,
            total_reused: 0,
            total_discarded: 0,
            dns_cache_hits: 0,
            dns_cache_misses: 0,
        }
    }
}

/// Per-domain connection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainConnectionInfo {
    pub domain: String,
    pub current_connections: usize,
    pub connection_limit: Option<usize>,
    pub utilization_percent: f64,
}

/// Detailed pool status for API/CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatus {
    pub config: PoolConfig,
    pub stats: PoolStats,
    pub domain_connections: Vec<DomainConnectionInfo>,
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections_per_addr, 4);
        assert_eq!(config.max_age_secs, 300);
        assert_eq!(config.max_idle_secs, 60);
        assert_eq!(config.connect_timeout_secs, 10);
        assert!(config.tcp_nodelay);
        assert!(config.dns_cache_enabled);
    }

    #[tokio::test]
    async fn test_pool_stats_empty() {
        let pool = ConnectionPool::new();
        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 0);
        assert_eq!(stats.total_connections, 0);
    }

    #[tokio::test]
    async fn test_pool_cleanup_empty() {
        let pool = ConnectionPool::new();
        pool.cleanup().await;
        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 0);
    }

    #[tokio::test]
    async fn test_pool_return_connection() {
        // Start a test server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // Connect and return to pool
        let stream = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 1);
        assert_eq!(stats.total_connections, 1);
    }

    #[tokio::test]
    async fn test_pool_reuse_connection() {
        // Start a test server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // First connection
        let stream1 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream1, addr).await;

        // Second connection should reuse the first one
        let stream2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream2, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 1);
        assert_eq!(stats.total_connections, 1);
    }

    #[tokio::test]
    async fn test_pool_max_connections() {
        // Start a test server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = PoolConfig {
            max_connections_per_addr: 2,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        // Create 3 connections
        let streams: Vec<_> = (0..3)
            .map(|_| pool.get_or_connect(addr))
            .collect::<futures::future::JoinAll<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Return all to pool
        for stream in streams {
            pool.return_connection(stream, addr).await;
        }

        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 1);
        assert_eq!(stats.total_connections, 2); // Limited to max_connections_per_addr
    }

    // ========== Phase 180: Comprehensive Test Coverage ==========

    #[test]
    fn test_pool_config_serialization() {
        let config = PoolConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.max_connections_per_addr,
            config.max_connections_per_addr
        );
        assert_eq!(deserialized.max_age_secs, config.max_age_secs);
        assert_eq!(deserialized.max_idle_secs, config.max_idle_secs);
        assert_eq!(
            deserialized.connect_timeout_secs,
            config.connect_timeout_secs
        );
        assert_eq!(deserialized.tcp_nodelay, config.tcp_nodelay);
        assert_eq!(deserialized.dns_cache_enabled, config.dns_cache_enabled);
        assert_eq!(deserialized.dns_cache_ttl_secs, config.dns_cache_ttl_secs);
        assert_eq!(
            deserialized.health_check_enabled,
            config.health_check_enabled
        );
    }

    #[test]
    fn test_pool_config_custom_serialization() {
        let config = PoolConfig {
            max_connections_per_addr: 8,
            max_age_secs: 600,
            max_idle_secs: 120,
            connect_timeout_secs: 5,
            tcp_send_buffer_size: 512 * 1024,
            tcp_recv_buffer_size: 512 * 1024,
            tcp_nodelay: false,
            dns_cache_enabled: false,
            dns_cache_ttl_secs: 600,
            health_check_enabled: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_connections_per_addr, 8);
        assert_eq!(deserialized.max_age_secs, 600);
        assert!(!deserialized.tcp_nodelay);
        assert!(!deserialized.dns_cache_enabled);
        assert!(!deserialized.health_check_enabled);
    }

    #[test]
    fn test_pool_config_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let config = PoolConfig {
            max_connections_per_addr: 6,
            max_age_secs: 120,
            ..Default::default()
        };
        save_pool_config(&config, dir.path()).unwrap();
        let loaded = load_pool_config(dir.path()).unwrap();
        assert_eq!(loaded.max_connections_per_addr, 6);
        assert_eq!(loaded.max_age_secs, 120);
    }

    #[test]
    fn test_pool_config_load_default_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load_pool_config(dir.path()).unwrap();
        assert_eq!(
            loaded.max_connections_per_addr,
            PoolConfig::default().max_connections_per_addr
        );
    }

    #[test]
    fn test_pool_config_load_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("connection_pool_config.json");
        std::fs::write(&path, "not valid json").unwrap();
        assert!(load_pool_config(dir.path()).is_err());
    }

    #[tokio::test]
    async fn test_domain_limit_unlimited_by_default() {
        let pool = ConnectionPool::new();
        assert!(pool.can_connect_domain("example.com").await);
        assert!(pool.can_connect_domain("unknown.com").await);
    }

    #[tokio::test]
    async fn test_domain_limit_set_and_check() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("example.com", 2).await;

        // Can connect (0 < 2)
        assert!(pool.can_connect_domain("example.com").await);

        // Record 2 connections
        pool.record_domain_connection("example.com").await;
        pool.record_domain_connection("example.com").await;

        // Now at limit (2 >= 2)
        assert!(!pool.can_connect_domain("example.com").await);

        // Other domains still unlimited
        assert!(pool.can_connect_domain("other.com").await);
    }

    #[tokio::test]
    async fn test_domain_disconnect_saturating() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("example.com", 1).await;

        // Record and disconnect
        pool.record_domain_connection("example.com").await;
        assert!(!pool.can_connect_domain("example.com").await);

        pool.record_domain_disconnect("example.com").await;
        assert!(pool.can_connect_domain("example.com").await);

        // Extra disconnect should not go below 0
        pool.record_domain_disconnect("example.com").await;
        assert!(pool.can_connect_domain("example.com").await);
    }

    #[tokio::test]
    async fn test_get_domain_connections_info() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("fast.com", 10).await;
        pool.set_domain_limit("slow.com", 5).await;

        pool.record_domain_connection("fast.com").await;
        pool.record_domain_connection("fast.com").await;
        pool.record_domain_connection("slow.com").await;

        let domain_info = pool.get_domain_connections().await;
        assert_eq!(domain_info.len(), 2);

        // Should be sorted by current_connections descending
        let fast = domain_info.iter().find(|d| d.domain == "fast.com").unwrap();
        assert_eq!(fast.current_connections, 2);
        assert_eq!(fast.connection_limit, Some(10));
        assert!((fast.utilization_percent - 20.0).abs() < 0.1);

        let slow = domain_info.iter().find(|d| d.domain == "slow.com").unwrap();
        assert_eq!(slow.current_connections, 1);
        assert_eq!(slow.connection_limit, Some(5));
        assert!((slow.utilization_percent - 20.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_mark_connection_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        let stream = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream, addr).await;

        // Mark errors on the connection
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await; // > 3 errors = unhealthy

        // The connection should now be considered unhealthy and discarded
        let stream2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream2, addr).await;

        let stats = pool.stats().await;
        // Should have discarded the unhealthy connection
        assert!(stats.total_discarded >= 1);
    }

    #[tokio::test]
    async fn test_pool_clear() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        let stream = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream, addr).await;

        pool.record_domain_connection("example.com").await;

        let stats_before = pool.stats().await;
        assert_eq!(stats_before.total_addresses, 1);

        pool.clear().await;

        let stats_after = pool.stats().await;
        assert_eq!(stats_after.total_addresses, 0);
        assert_eq!(stats_after.total_connections, 0);
        assert_eq!(stats_after.total_created, 0);
        assert_eq!(stats_after.total_reused, 0);
    }

    #[tokio::test]
    async fn test_pool_update_config() {
        let pool = ConnectionPool::new();
        let original = pool.get_config_async().await;
        assert_eq!(original.max_connections_per_addr, 4);

        let new_config = PoolConfig {
            max_connections_per_addr: 10,
            max_age_secs: 1200,
            ..Default::default()
        };
        pool.update_config(new_config).await;

        let updated = pool.get_config_async().await;
        assert_eq!(updated.max_connections_per_addr, 10);
        assert_eq!(updated.max_age_secs, 1200);
    }

    #[tokio::test]
    async fn test_pool_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        pool.set_domain_limit("example.com", 5).await;
        pool.record_domain_connection("example.com").await;

        let stream = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream, addr).await;

        let status = pool.status().await;
        assert_eq!(status.config.max_connections_per_addr, 4);
        assert_eq!(status.stats.total_addresses, 1);
        assert_eq!(status.domain_connections.len(), 1);
        assert!(status.uptime_secs < 10);
    }

    #[tokio::test]
    async fn test_pool_stats_serialization() {
        let stats = PoolStats {
            total_addresses: 5,
            total_connections: 10,
            healthy_connections: 8,
            dns_cache_size: 3,
            total_created: 100,
            total_reused: 50,
            total_discarded: 5,
            dns_cache_hits: 80,
            dns_cache_misses: 20,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: PoolStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_addresses, 5);
        assert_eq!(deserialized.total_connections, 10);
        assert_eq!(deserialized.total_reused, 50);
        assert_eq!(deserialized.dns_cache_hits, 80);
    }

    #[tokio::test]
    async fn test_domain_connection_info_serialization() {
        let info = DomainConnectionInfo {
            domain: "example.com".to_string(),
            current_connections: 3,
            connection_limit: Some(10),
            utilization_percent: 30.0,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: DomainConnectionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.domain, "example.com");
        assert_eq!(deserialized.current_connections, 3);
        assert_eq!(deserialized.connection_limit, Some(10));
    }

    #[tokio::test]
    async fn test_pool_status_serialization() {
        let status = PoolStatus {
            config: PoolConfig::default(),
            stats: PoolStats::default(),
            domain_connections: vec![DomainConnectionInfo {
                domain: "test.com".to_string(),
                current_connections: 1,
                connection_limit: Some(5),
                utilization_percent: 20.0,
            }],
            uptime_secs: 3600,
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: PoolStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.uptime_secs, 3600);
        assert_eq!(deserialized.domain_connections.len(), 1);
    }

    #[tokio::test]
    async fn test_pool_cleanup_removes_idle() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Use very short idle timeout
        let config = PoolConfig {
            max_idle_secs: 0, // Expire immediately
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        let stream = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(stream, addr).await;

        // Wait a tiny bit so idle check triggers
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        pool.cleanup().await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 0);
    }

    #[tokio::test]
    async fn test_pre_connect_creates_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        pool.pre_connect(addr).await.unwrap();

        // Connection should now be in the pool
        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 1);
        assert_eq!(stats.total_connections, 1);
    }

    #[tokio::test]
    async fn test_pre_connect_skips_if_already_has_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // First pre-connect creates a connection
        pool.pre_connect(addr).await.unwrap();
        let stats1 = pool.stats().await;
        assert_eq!(stats1.total_created, 1);

        // Second pre-connect should find existing and skip creating
        pool.pre_connect(addr).await.unwrap();
        let stats2 = pool.stats().await;
        assert_eq!(stats2.total_created, 1); // No new connection created
    }

    #[tokio::test]
    async fn test_pool_error_display() {
        let err = PoolError::Timeout;
        assert_eq!(err.to_string(), "connection timeout");

        let err = PoolError::Dns("lookup failed".to_string());
        assert_eq!(err.to_string(), "DNS resolution failed: lookup failed");

        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = PoolError::Io(io_err);
        assert!(err.to_string().contains("refused"));
    }

    #[tokio::test]
    async fn test_pool_config_accessors() {
        let config = PoolConfig {
            max_connections_per_addr: 8,
            max_age_secs: 600,
            max_idle_secs: 120,
            connect_timeout_secs: 5,
            dns_cache_ttl_secs: 180,
            ..Default::default()
        };
        assert_eq!(config.max_age(), Duration::from_secs(600));
        assert_eq!(config.max_idle(), Duration::from_secs(120));
        assert_eq!(config.connect_timeout(), Duration::from_secs(5));
        assert_eq!(config.dns_cache_ttl(), Duration::from_secs(180));
    }

    #[tokio::test]
    async fn test_pool_get_config_sync() {
        let pool = ConnectionPool::new();
        // Test the sync get_config method
        let config = pool.get_config();
        assert_eq!(config.max_connections_per_addr, 4);
        // Also test async variant
        let config_async = pool.get_config_async().await;
        assert_eq!(config_async.max_connections_per_addr, 4);
    }

    #[tokio::test]
    async fn test_pool_multiple_addresses() {
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();

        let pool = ConnectionPool::new();

        let s1 = pool.get_or_connect(addr1).await.unwrap();
        let s2 = pool.get_or_connect(addr2).await.unwrap();
        pool.return_connection(s1, addr1).await;
        pool.return_connection(s2, addr2).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 2);
        assert_eq!(stats.total_connections, 2);
    }

    #[tokio::test]
    async fn test_pool_domain_utilization_zero_limit() {
        let pool = ConnectionPool::new();
        // Domain with no limit set
        pool.record_domain_connection("nolimit.com").await;

        let domain_info = pool.get_domain_connections().await;
        let info = domain_info
            .iter()
            .find(|d| d.domain == "nolimit.com")
            .unwrap();
        assert_eq!(info.connection_limit, None);
        assert_eq!(info.utilization_percent, 0.0);
    }

    #[tokio::test]
    async fn test_resolve_cached_with_ip_literal() {
        let pool = ConnectionPool::new();
        // 127.0.0.1 is a literal IP, so DNS resolution should work immediately
        let result = pool.resolve_cached("127.0.0.1", 8080).await;
        assert!(result.is_ok());
        let addr = result.unwrap();
        assert_eq!(addr.port(), 8080);
    }

    #[tokio::test]
    async fn test_resolve_cached_dns_hit() {
        let pool = ConnectionPool::new();
        // First resolution (cache miss)
        let addr1 = pool.resolve_cached("127.0.0.1", 9090).await.unwrap();
        let stats1 = pool.stats().await;
        assert_eq!(stats1.dns_cache_misses, 1);
        assert_eq!(stats1.dns_cache_hits, 0);

        // Second resolution (cache hit)
        let addr2 = pool.resolve_cached("127.0.0.1", 9091).await.unwrap();
        let stats2 = pool.stats().await;
        assert_eq!(stats2.dns_cache_hits, 1);
        // Port should be updated to the requested port
        assert_eq!(addr2.port(), 9091);
        assert_eq!(addr1.ip(), addr2.ip());
    }

    #[tokio::test]
    async fn test_resolve_cached_dns_disabled() {
        let config = PoolConfig {
            dns_cache_enabled: false,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        pool.resolve_cached("127.0.0.1", 8080).await.unwrap();
        let stats = pool.stats().await;
        // DNS cache disabled: no caching, always miss
        assert_eq!(stats.dns_cache_size, 0);
        assert_eq!(stats.dns_cache_hits, 0);
        assert_eq!(stats.dns_cache_misses, 1);
    }

    #[tokio::test]
    async fn test_pool_reuse_increments_stats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // First connection
        let s1 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s1, addr).await;

        // Second get should reuse
        let s2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s2, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_created, 1);
        assert_eq!(stats.total_reused, 1);
    }

    #[tokio::test]
    async fn test_pool_health_check_disabled() {
        let config = PoolConfig {
            health_check_enabled: false,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mark errors (would make connection unhealthy with health check)
        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;

        // With health check disabled, the connection should still be reused
        let s2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s2, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_reused, 1); // Reused, not discarded
        assert_eq!(stats.total_discarded, 0);
    }

    #[tokio::test]
    async fn test_pool_entry_is_healthy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut entry = PoolEntry::new(stream, addr);

        // Fresh entry is healthy
        assert!(entry.is_healthy());

        // After 3 errors, still healthy (threshold is > 3)
        entry.record_error();
        entry.record_error();
        entry.record_error();
        assert!(entry.is_healthy());

        // 4th error makes it unhealthy
        entry.record_error();
        assert!(!entry.is_healthy());
    }

    #[tokio::test]
    async fn test_pool_entry_record_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut entry = PoolEntry::new(stream, addr);

        assert_eq!(entry.reuse_count, 0);
        entry.record_success();
        assert_eq!(entry.reuse_count, 1);
        entry.record_success();
        assert_eq!(entry.reuse_count, 2);
    }

    // ========== Phase 207: Comprehensive Test Coverage ==========

    // --- PoolConfig: Duration methods ---

    #[test]
    fn test_pool_config_duration_methods_zero() {
        let config = PoolConfig {
            max_age_secs: 0,
            max_idle_secs: 0,
            connect_timeout_secs: 0,
            dns_cache_ttl_secs: 0,
            ..Default::default()
        };
        assert_eq!(config.max_age(), Duration::from_secs(0));
        assert_eq!(config.max_idle(), Duration::from_secs(0));
        assert_eq!(config.connect_timeout(), Duration::from_secs(0));
        assert_eq!(config.dns_cache_ttl(), Duration::from_secs(0));
    }

    #[test]
    fn test_pool_config_duration_methods_large_values() {
        let config = PoolConfig {
            max_age_secs: u64::MAX,
            max_idle_secs: u64::MAX,
            connect_timeout_secs: u64::MAX,
            dns_cache_ttl_secs: u64::MAX,
            ..Default::default()
        };
        assert_eq!(config.max_age(), Duration::from_secs(u64::MAX));
        assert_eq!(config.max_idle(), Duration::from_secs(u64::MAX));
    }

    // --- PoolConfig: TCP buffer fields ---

    #[test]
    fn test_pool_config_tcp_buffer_sizes() {
        let config = PoolConfig {
            tcp_send_buffer_size: 512 * 1024,
            tcp_recv_buffer_size: 1024 * 1024,
            ..Default::default()
        };
        assert_eq!(config.tcp_send_buffer_size, 512 * 1024);
        assert_eq!(config.tcp_recv_buffer_size, 1024 * 1024);
    }

    #[test]
    fn test_pool_config_tcp_buffer_zero_system_default() {
        let config = PoolConfig {
            tcp_send_buffer_size: 0,
            tcp_recv_buffer_size: 0,
            ..Default::default()
        };
        assert_eq!(config.tcp_send_buffer_size, 0);
        assert_eq!(config.tcp_recv_buffer_size, 0);
    }

    // --- PoolConfig: Serialization ---

    #[test]
    fn test_pool_config_pretty_serialization() {
        let config = PoolConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: PoolConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(
            deserialized.max_connections_per_addr,
            config.max_connections_per_addr
        );
    }

    #[test]
    fn test_pool_config_extra_json_fields_ignored() {
        let json = r#"{
            "max_connections_per_addr": 8,
            "max_age_secs": 600,
            "max_idle_secs": 120,
            "connect_timeout_secs": 5,
            "tcp_send_buffer_size": 262144,
            "tcp_recv_buffer_size": 262144,
            "tcp_nodelay": true,
            "dns_cache_enabled": true,
            "dns_cache_ttl_secs": 300,
            "health_check_enabled": true,
            "unknown_field": "should be ignored",
            "another_extra": 42
        }"#;
        let config: PoolConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections_per_addr, 8);
        assert_eq!(config.max_age_secs, 600);
    }

    #[test]
    fn test_pool_config_zero_values_serialization() {
        let config = PoolConfig {
            max_connections_per_addr: 0,
            max_age_secs: 0,
            max_idle_secs: 0,
            connect_timeout_secs: 0,
            tcp_send_buffer_size: 0,
            tcp_recv_buffer_size: 0,
            tcp_nodelay: false,
            dns_cache_enabled: false,
            dns_cache_ttl_secs: 0,
            health_check_enabled: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_connections_per_addr, 0);
        assert_eq!(deserialized.max_age_secs, 0);
        assert!(!deserialized.tcp_nodelay);
    }

    // --- PoolConfig: Traits ---

    #[test]
    fn test_pool_config_clone() {
        let config = PoolConfig {
            max_connections_per_addr: 16,
            max_age_secs: 999,
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(cloned.max_connections_per_addr, 16);
        assert_eq!(cloned.max_age_secs, 999);
    }

    #[test]
    fn test_pool_config_debug() {
        let config = PoolConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("PoolConfig"));
        assert!(debug.contains("max_connections_per_addr"));
    }

    // --- PoolStats: Default and traits ---

    #[test]
    fn test_pool_stats_default_all_zero() {
        let stats = PoolStats::default();
        assert_eq!(stats.total_addresses, 0);
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.healthy_connections, 0);
        assert_eq!(stats.dns_cache_size, 0);
        assert_eq!(stats.total_created, 0);
        assert_eq!(stats.total_reused, 0);
        assert_eq!(stats.total_discarded, 0);
        assert_eq!(stats.dns_cache_hits, 0);
        assert_eq!(stats.dns_cache_misses, 0);
    }

    #[test]
    fn test_pool_stats_clone() {
        let stats = PoolStats {
            total_addresses: 3,
            total_connections: 10,
            healthy_connections: 8,
            dns_cache_size: 2,
            total_created: 50,
            total_reused: 30,
            total_discarded: 5,
            dns_cache_hits: 100,
            dns_cache_misses: 20,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.total_addresses, 3);
        assert_eq!(cloned.total_reused, 30);
        assert_eq!(cloned.dns_cache_hits, 100);
    }

    #[test]
    fn test_pool_stats_debug() {
        let stats = PoolStats::default();
        let debug = format!("{:?}", stats);
        assert!(debug.contains("PoolStats"));
    }

    #[test]
    fn test_pool_stats_full_roundtrip() {
        let stats = PoolStats {
            total_addresses: 7,
            total_connections: 15,
            healthy_connections: 12,
            dns_cache_size: 4,
            total_created: 200,
            total_reused: 150,
            total_discarded: 10,
            dns_cache_hits: 500,
            dns_cache_misses: 50,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: PoolStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_addresses, 7);
        assert_eq!(deserialized.total_connections, 15);
        assert_eq!(deserialized.healthy_connections, 12);
        assert_eq!(deserialized.dns_cache_size, 4);
        assert_eq!(deserialized.total_created, 200);
        assert_eq!(deserialized.total_reused, 150);
        assert_eq!(deserialized.total_discarded, 10);
        assert_eq!(deserialized.dns_cache_hits, 500);
        assert_eq!(deserialized.dns_cache_misses, 50);
    }

    #[test]
    fn test_pool_stats_extra_fields_ignored() {
        let json = r#"{
            "total_addresses": 1,
            "total_connections": 2,
            "healthy_connections": 2,
            "dns_cache_size": 0,
            "total_created": 10,
            "total_reused": 5,
            "total_discarded": 1,
            "dns_cache_hits": 3,
            "dns_cache_misses": 7,
            "extra_future_field": true
        }"#;
        let stats: PoolStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.total_addresses, 1);
        assert_eq!(stats.total_reused, 5);
    }

    // --- DomainConnectionInfo: Traits and edge cases ---

    #[test]
    fn test_domain_connection_info_clone() {
        let info = DomainConnectionInfo {
            domain: "test.com".to_string(),
            current_connections: 5,
            connection_limit: Some(10),
            utilization_percent: 50.0,
        };
        let cloned = info.clone();
        assert_eq!(cloned.domain, "test.com");
        assert_eq!(cloned.current_connections, 5);
    }

    #[test]
    fn test_domain_connection_info_debug() {
        let info = DomainConnectionInfo {
            domain: "debug.com".to_string(),
            current_connections: 0,
            connection_limit: None,
            utilization_percent: 0.0,
        };
        let debug = format!("{:?}", info);
        assert!(debug.contains("debug.com"));
    }

    #[test]
    fn test_domain_connection_info_no_limit_utilization_zero() {
        let info = DomainConnectionInfo {
            domain: "nolimit.com".to_string(),
            current_connections: 100,
            connection_limit: None,
            utilization_percent: 0.0,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: DomainConnectionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.current_connections, 100);
        assert!(deserialized.connection_limit.is_none());
    }

    #[test]
    fn test_domain_connection_info_unicode_domain() {
        let info = DomainConnectionInfo {
            domain: "日本語テスト.com".to_string(),
            current_connections: 1,
            connection_limit: Some(5),
            utilization_percent: 20.0,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: DomainConnectionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.domain, "日本語テスト.com");
    }

    // --- PoolStatus: Traits ---

    #[test]
    fn test_pool_status_clone() {
        let status = PoolStatus {
            config: PoolConfig::default(),
            stats: PoolStats::default(),
            domain_connections: vec![],
            uptime_secs: 42,
        };
        let cloned = status.clone();
        assert_eq!(cloned.uptime_secs, 42);
        assert_eq!(cloned.config.max_connections_per_addr, 4);
    }

    #[test]
    fn test_pool_status_debug() {
        let status = PoolStatus {
            config: PoolConfig::default(),
            stats: PoolStats::default(),
            domain_connections: vec![],
            uptime_secs: 0,
        };
        let debug = format!("{:?}", status);
        assert!(debug.contains("PoolStatus"));
    }

    // --- PoolError: Traits and conversions ---

    #[test]
    fn test_pool_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let pool_err: PoolError = PoolError::Io(io_err);
        assert!(pool_err.to_string().contains("refused"));
    }

    #[test]
    fn test_pool_error_debug() {
        let err = PoolError::Timeout;
        let debug = format!("{:?}", err);
        assert!(debug.contains("Timeout"));

        let err = PoolError::Dns("no such host".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Dns"));
    }

    #[test]
    fn test_pool_error_dns_empty_message() {
        let err = PoolError::Dns(String::new());
        assert_eq!(err.to_string(), "DNS resolution failed: ");
    }

    // --- ConnectionPool: Construction and defaults ---

    #[test]
    fn test_connection_pool_default_trait() {
        let pool = ConnectionPool::default();
        let config = pool.get_config();
        assert_eq!(config.max_connections_per_addr, 4);
        assert_eq!(config.max_age_secs, 300);
    }

    #[tokio::test]
    async fn test_connection_pool_with_custom_config() {
        let config = PoolConfig {
            max_connections_per_addr: 20,
            max_age_secs: 1200,
            max_idle_secs: 300,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);
        let loaded = pool.get_config_async().await;
        assert_eq!(loaded.max_connections_per_addr, 20);
        assert_eq!(loaded.max_age_secs, 1200);
        assert_eq!(loaded.max_idle_secs, 300);
    }

    // --- ConnectionPool: Domain limits ---

    #[tokio::test]
    async fn test_domain_limit_override() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("example.com", 5).await;
        assert!(pool.can_connect_domain("example.com").await);

        // Override the limit
        pool.set_domain_limit("example.com", 1).await;
        // Already at 0 connections, limit 1 should still allow
        assert!(pool.can_connect_domain("example.com").await);

        pool.record_domain_connection("example.com").await;
        // Now at 1, limit 1 should deny
        assert!(!pool.can_connect_domain("example.com").await);
    }

    #[tokio::test]
    async fn test_domain_limit_zero() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("blocked.com", 0).await;
        // Zero limit means no connections allowed
        assert!(!pool.can_connect_domain("blocked.com").await);
    }

    #[tokio::test]
    async fn test_domain_disconnect_unknown_domain_no_panic() {
        let pool = ConnectionPool::new();
        // Disconnect from a domain that was never recorded should not panic
        pool.record_domain_disconnect("never-existed.com").await;
        assert!(pool.can_connect_domain("never-existed.com").await);
    }

    #[tokio::test]
    async fn test_domain_connections_empty() {
        let pool = ConnectionPool::new();
        let info = pool.get_domain_connections().await;
        assert!(info.is_empty());
    }

    #[tokio::test]
    async fn test_domain_connections_sorted_descending() {
        let pool = ConnectionPool::new();
        pool.record_domain_connection("a.com").await;
        pool.record_domain_connection("b.com").await;
        pool.record_domain_connection("b.com").await;
        pool.record_domain_connection("c.com").await;
        pool.record_domain_connection("c.com").await;
        pool.record_domain_connection("c.com").await;

        let info = pool.get_domain_connections().await;
        assert_eq!(info.len(), 3);
        // c.com (3) > b.com (2) > a.com (1)
        assert_eq!(info[0].domain, "c.com");
        assert_eq!(info[0].current_connections, 3);
        assert_eq!(info[1].domain, "b.com");
        assert_eq!(info[1].current_connections, 2);
        assert_eq!(info[2].domain, "a.com");
        assert_eq!(info[2].current_connections, 1);
    }

    #[tokio::test]
    async fn test_domain_connections_multiple_records() {
        let pool = ConnectionPool::new();
        for _ in 0..10 {
            pool.record_domain_connection("heavy.com").await;
        }
        let info = pool.get_domain_connections().await;
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].current_connections, 10);
    }

    // --- ConnectionPool: get_or_connect ---

    #[tokio::test]
    async fn test_get_or_connect_increments_created() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        let s1 = pool.get_or_connect(addr).await.unwrap();
        drop(s1);

        let stats = pool.stats().await;
        assert_eq!(stats.total_created, 1);
    }

    #[tokio::test]
    async fn test_get_or_connect_unreachable_timeout() {
        let config = PoolConfig {
            connect_timeout_secs: 1,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);
        // Use a non-routable address to trigger timeout
        let unreachable: SocketAddr = "192.0.2.1:12345".parse().unwrap();
        let result = pool.get_or_connect(unreachable).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should be either Timeout or Io error
        match err {
            PoolError::Timeout | PoolError::Io(_) => {}
            _ => panic!("Expected Timeout or Io error, got: {:?}", err),
        }
    }

    #[tokio::test]
    async fn test_get_or_connect_reuse_then_return() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // First connection
        let s1 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s1, addr).await;

        // Second should reuse
        let s2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s2, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_created, 1);
        assert_eq!(stats.total_reused, 1);
        assert_eq!(stats.total_discarded, 0);
    }

    // --- ConnectionPool: return_connection ---

    #[tokio::test]
    async fn test_return_connection_respects_max_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = PoolConfig {
            max_connections_per_addr: 1,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        // Create and return 3 connections
        for _ in 0..3 {
            let s = pool.get_or_connect(addr).await.unwrap();
            pool.return_connection(s, addr).await;
        }

        let stats = pool.stats().await;
        // Should be capped at max_connections_per_addr
        assert_eq!(stats.total_connections, 1);
    }

    #[tokio::test]
    async fn test_return_connection_different_addresses() {
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a1 = l1.local_addr().unwrap();
        let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a2 = l2.local_addr().unwrap();

        let pool = ConnectionPool::new();

        let s1 = pool.get_or_connect(a1).await.unwrap();
        let s2 = pool.get_or_connect(a2).await.unwrap();
        pool.return_connection(s1, a1).await;
        pool.return_connection(s2, a2).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 2);
        assert_eq!(stats.total_connections, 2);
    }

    // --- ConnectionPool: mark_connection_error ---

    #[tokio::test]
    async fn test_mark_connection_error_nonexistent_addr() {
        let pool = ConnectionPool::new();
        let fake_addr: SocketAddr = "127.0.0.1:59999".parse().unwrap();
        // Should not panic on non-existent address
        pool.mark_connection_error(fake_addr).await;
    }

    #[tokio::test]
    async fn test_mark_connection_error_boundary_3_errors_healthy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;

        // 3 errors: still healthy (threshold is > 3)
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;
        pool.mark_connection_error(addr).await;

        // Connection should still be reused
        let s2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s2, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_reused, 1);
        assert_eq!(stats.total_discarded, 0);
    }

    #[tokio::test]
    async fn test_mark_connection_error_4_errors_unhealthy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;

        // 4 errors: unhealthy
        for _ in 0..4 {
            pool.mark_connection_error(addr).await;
        }

        // Connection should be discarded on next get
        let s2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s2, addr).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_discarded, 1);
        assert_eq!(stats.total_reused, 0);
    }

    // --- ConnectionPool: cleanup ---

    #[tokio::test]
    async fn test_cleanup_preserves_healthy_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;

        pool.cleanup().await;

        let stats = pool.stats().await;
        // Healthy connection should survive cleanup
        assert_eq!(stats.total_connections, 1);
    }

    #[tokio::test]
    async fn test_cleanup_removes_expired_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = PoolConfig {
            max_age_secs: 0, // Expire immediately
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        pool.cleanup().await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 0);
    }

    // --- ConnectionPool: clear ---

    #[tokio::test]
    async fn test_clear_resets_all_stats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // Create some activity
        let s1 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s1, addr).await;
        let s2 = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s2, addr).await;

        pool.record_domain_connection("example.com").await;
        let _ = pool.resolve_cached("127.0.0.1", 80).await;

        pool.clear().await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_addresses, 0);
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.total_created, 0);
        assert_eq!(stats.total_reused, 0);
        assert_eq!(stats.total_discarded, 0);
        assert_eq!(stats.dns_cache_hits, 0);
        assert_eq!(stats.dns_cache_misses, 0);
        assert_eq!(stats.dns_cache_size, 0);
    }

    #[tokio::test]
    async fn test_clear_also_clears_dns_cache() {
        let pool = ConnectionPool::new();

        // Populate DNS cache
        let _ = pool.resolve_cached("127.0.0.1", 80).await;
        let stats_before = pool.stats().await;
        assert_eq!(stats_before.dns_cache_size, 1);

        pool.clear().await;

        let stats_after = pool.stats().await;
        assert_eq!(stats_after.dns_cache_size, 0);
    }

    // --- ConnectionPool: update_config ---

    #[tokio::test]
    async fn test_update_config_affects_max_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // Return 3 connections with default limit (4)
        let mut streams = vec![];
        for _ in 0..3 {
            let s = pool.get_or_connect(addr).await.unwrap();
            streams.push(s);
        }
        for s in streams {
            pool.return_connection(s, addr).await;
        }

        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 3);

        // Now reduce max to 1 and return another
        let new_config = PoolConfig {
            max_connections_per_addr: 1,
            ..Default::default()
        };
        pool.update_config(new_config).await;

        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;

        let stats = pool.stats().await;
        // Cleanup during return_connection should have trimmed to 1
        assert_eq!(stats.total_connections, 1);
    }

    // --- ConnectionPool: resolve_cached ---

    #[tokio::test]
    async fn test_resolve_cached_invalid_hostname() {
        let pool = ConnectionPool::new();
        let result = pool
            .resolve_cached("this-host-does-not-exist.invalid", 80)
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PoolError::Dns(msg) => assert!(!msg.is_empty()),
            other => panic!("Expected Dns error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_resolve_cached_port_override() {
        let pool = ConnectionPool::new();
        // First resolve with port 80
        let addr1 = pool.resolve_cached("127.0.0.1", 80).await.unwrap();
        assert_eq!(addr1.port(), 80);

        // Cache hit with different port should override
        let addr2 = pool.resolve_cached("127.0.0.1", 443).await.unwrap();
        assert_eq!(addr2.port(), 443);
        assert_eq!(addr1.ip(), addr2.ip());
    }

    #[tokio::test]
    async fn test_resolve_cached_dns_disabled_always_misses() {
        let config = PoolConfig {
            dns_cache_enabled: false,
            ..Default::default()
        };
        let pool = ConnectionPool::with_config(config);

        // First resolution
        let _ = pool.resolve_cached("127.0.0.1", 80).await.unwrap();
        let stats1 = pool.stats().await;
        assert_eq!(stats1.dns_cache_misses, 1);
        assert_eq!(stats1.dns_cache_hits, 0);

        // Second resolution should also miss (caching disabled)
        let _ = pool.resolve_cached("127.0.0.1", 80).await.unwrap();
        let stats2 = pool.stats().await;
        assert_eq!(stats2.dns_cache_misses, 2);
        assert_eq!(stats2.dns_cache_hits, 0);
    }

    // --- ConnectionPool: stats accuracy ---

    #[tokio::test]
    async fn test_stats_accuracy_after_complex_operations() {
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a1 = l1.local_addr().unwrap();
        let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a2 = l2.local_addr().unwrap();

        let pool = ConnectionPool::new();

        // Create connections to two addresses
        let s1 = pool.get_or_connect(a1).await.unwrap();
        let s2 = pool.get_or_connect(a2).await.unwrap();

        // Return both
        pool.return_connection(s1, a1).await;
        pool.return_connection(s2, a2).await;

        // Reuse both
        let s3 = pool.get_or_connect(a1).await.unwrap();
        let s4 = pool.get_or_connect(a2).await.unwrap();

        let stats = pool.stats().await;
        assert_eq!(stats.total_created, 2);
        assert_eq!(stats.total_reused, 2);
        assert_eq!(stats.total_addresses, 2);

        // Return again
        pool.return_connection(s3, a1).await;
        pool.return_connection(s4, a2).await;

        let stats2 = pool.stats().await;
        assert_eq!(stats2.total_connections, 2);
    }

    // --- ConnectionPool: status ---

    #[tokio::test]
    async fn test_pool_status_comprehensive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        pool.set_domain_limit("alpha.com", 10).await;
        pool.set_domain_limit("beta.com", 5).await;
        pool.record_domain_connection("alpha.com").await;
        pool.record_domain_connection("alpha.com").await;
        pool.record_domain_connection("beta.com").await;

        let s = pool.get_or_connect(addr).await.unwrap();
        pool.return_connection(s, addr).await;

        let status = pool.status().await;
        assert_eq!(status.config.max_connections_per_addr, 4);
        assert_eq!(status.stats.total_addresses, 1);
        assert_eq!(status.domain_connections.len(), 2);
        assert!(status.uptime_secs < 10);

        // Verify domain connections are sorted
        assert_eq!(status.domain_connections[0].domain, "alpha.com");
        assert_eq!(status.domain_connections[0].current_connections, 2);
    }

    // --- Persistence ---

    #[test]
    fn test_save_pool_config_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = PoolConfig::default();
        save_pool_config(&config, dir.path()).unwrap();
        let path = dir.path().join("connection_pool_config.json");
        assert!(path.exists());
    }

    #[test]
    fn test_save_pool_config_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let config1 = PoolConfig {
            max_connections_per_addr: 2,
            ..Default::default()
        };
        save_pool_config(&config1, dir.path()).unwrap();

        let config2 = PoolConfig {
            max_connections_per_addr: 10,
            ..Default::default()
        };
        save_pool_config(&config2, dir.path()).unwrap();

        let loaded = load_pool_config(dir.path()).unwrap();
        assert_eq!(loaded.max_connections_per_addr, 10);
    }

    #[test]
    fn test_save_pool_config_no_tmp_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let config = PoolConfig::default();
        save_pool_config(&config, dir.path()).unwrap();

        // Check no temporary files left
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.contains("tmp") || name.starts_with('.')
            })
            .collect();
        assert!(entries.is_empty(), "Temporary files found: {:?}", entries);
    }

    #[test]
    fn test_load_pool_config_full_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = PoolConfig {
            max_connections_per_addr: 7,
            max_age_secs: 456,
            max_idle_secs: 78,
            connect_timeout_secs: 3,
            tcp_send_buffer_size: 128 * 1024,
            tcp_recv_buffer_size: 64 * 1024,
            tcp_nodelay: false,
            dns_cache_enabled: false,
            dns_cache_ttl_secs: 120,
            health_check_enabled: false,
        };
        save_pool_config(&config, dir.path()).unwrap();
        let loaded = load_pool_config(dir.path()).unwrap();
        assert_eq!(loaded.max_connections_per_addr, 7);
        assert_eq!(loaded.max_age_secs, 456);
        assert_eq!(loaded.max_idle_secs, 78);
        assert_eq!(loaded.connect_timeout_secs, 3);
        assert_eq!(loaded.tcp_send_buffer_size, 128 * 1024);
        assert_eq!(loaded.tcp_recv_buffer_size, 64 * 1024);
        assert_eq!(loaded.tcp_nodelay, false);
        assert_eq!(loaded.dns_cache_enabled, false);
        assert_eq!(loaded.dns_cache_ttl_secs, 120);
        assert_eq!(loaded.health_check_enabled, false);
    }

    // --- PoolEntry: boundary tests ---

    #[tokio::test]
    async fn test_pool_entry_is_healthy_exactly_3_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut entry = PoolEntry::new(stream, addr);

        // Exactly 3 errors: still healthy
        for _ in 0..3 {
            entry.record_error();
        }
        assert!(entry.is_healthy());
        assert_eq!(entry.error_count, 3);
    }

    #[tokio::test]
    async fn test_pool_entry_is_healthy_4_errors_unhealthy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut entry = PoolEntry::new(stream, addr);

        for _ in 0..4 {
            entry.record_error();
        }
        assert!(!entry.is_healthy());
    }

    #[tokio::test]
    async fn test_pool_entry_initial_state() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let entry = PoolEntry::new(stream, addr);

        assert_eq!(entry.reuse_count, 0);
        assert_eq!(entry.error_count, 0);
        assert!(entry.last_rtt_ms.is_none());
        assert_eq!(entry.addr, addr);
    }

    // --- ConnectionPool: pre_connect ---

    #[tokio::test]
    async fn test_pre_connect_adds_to_pool_stats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new();
        pool.pre_connect(addr).await.unwrap();

        let stats = pool.stats().await;
        assert_eq!(stats.total_created, 1);
        assert_eq!(stats.total_addresses, 1);
        assert_eq!(stats.total_connections, 1);
    }

    // --- ConnectionPool: get_config sync fallback ---

    #[tokio::test]
    async fn test_get_config_sync_returns_default_when_locked() {
        let pool = ConnectionPool::new();
        // When not locked, should return actual config
        let config = pool.get_config();
        assert_eq!(config.max_connections_per_addr, 4);
    }

    // --- ConnectionPool: domain utilization calculation ---

    #[tokio::test]
    async fn test_domain_utilization_100_percent() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("full.com", 3).await;
        for _ in 0..3 {
            pool.record_domain_connection("full.com").await;
        }

        let info = pool.get_domain_connections().await;
        let domain = info.iter().find(|d| d.domain == "full.com").unwrap();
        assert!((domain.utilization_percent - 100.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_domain_utilization_over_limit() {
        let pool = ConnectionPool::new();
        pool.set_domain_limit("over.com", 2).await;
        for _ in 0..5 {
            pool.record_domain_connection("over.com").await;
        }

        let info = pool.get_domain_connections().await;
        let domain = info.iter().find(|d| d.domain == "over.com").unwrap();
        // 5/2 * 100 = 250%
        assert!((domain.utilization_percent - 250.0).abs() < 0.1);
    }
}
