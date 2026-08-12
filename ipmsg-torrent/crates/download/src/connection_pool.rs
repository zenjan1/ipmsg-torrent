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
pub fn save_pool_config(config: &PoolConfig, data_dir: &std::path::Path) -> Result<(), std::io::Error> {
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
    serde_json::from_str(&json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
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
        self.config.try_lock().map(|g| g.clone()).unwrap_or_default()
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
}
