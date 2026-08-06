//! Connection pool for reusing TCP connections
//!
//! This module provides a simple connection pool that can be used by
//! different download engines to reuse TCP connections.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Connection pool entry
#[allow(dead_code)]
struct PoolEntry {
    stream: TcpStream,
    addr: SocketAddr,
    created_at: Instant,
    last_used: Instant,
}

impl PoolEntry {
    fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            stream,
            addr,
            created_at: now,
            last_used: now,
        }
    }

    fn is_expired(&self, max_age: Duration) -> bool {
        self.created_at.elapsed() > max_age
    }

    fn is_idle(&self, max_idle: Duration) -> bool {
        self.last_used.elapsed() > max_idle
    }
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum connections per address
    pub max_connections_per_addr: usize,
    /// Maximum age of a connection
    pub max_age: Duration,
    /// Maximum idle time before connection is closed
    pub max_idle: Duration,
    /// Connection timeout
    pub connect_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_addr: 4,
            max_age: Duration::from_secs(300), // 5 minutes
            max_idle: Duration::from_secs(60), // 1 minute
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// Connection pool for reusing TCP connections
pub struct ConnectionPool {
    config: PoolConfig,
    connections: Arc<Mutex<HashMap<SocketAddr, Vec<PoolEntry>>>>,
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionPool {
    /// Create a new connection pool with default configuration
    pub fn new() -> Self {
        Self::with_config(PoolConfig::default())
    }

    /// Create a new connection pool with custom configuration
    pub fn with_config(config: PoolConfig) -> Self {
        Self {
            config,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a connection from the pool or create a new one
    pub async fn get_or_connect(&self, addr: SocketAddr) -> Result<TcpStream, PoolError> {
        // Try to get an existing connection
        {
            let mut conns = self.connections.lock().await;
            if let Some(entries) = conns.get_mut(&addr) {
                // Remove expired or idle connections
                entries.retain(|e| {
                    !e.is_expired(self.config.max_age) && !e.is_idle(self.config.max_idle)
                });

                // Try to get a connection
                if let Some(entry) = entries.pop() {
                    // Clean up empty entries
                    if entries.is_empty() {
                        conns.remove(&addr);
                    }
                    return Ok(entry.stream);
                }
            }
        }

        // Create a new connection
        self.connect(addr).await
    }

    /// Return a connection to the pool
    pub async fn return_connection(&self, stream: TcpStream, addr: SocketAddr) {
        let entry = PoolEntry::new(stream, addr);

        let mut conns = self.connections.lock().await;
        let entries = conns.entry(addr).or_insert_with(Vec::new);

        // Remove expired connections
        entries.retain(|e| !e.is_expired(self.config.max_age) && !e.is_idle(self.config.max_idle));

        // Add the new entry if we haven't reached the limit
        if entries.len() < self.config.max_connections_per_addr {
            entries.push(entry);
        }
    }

    /// Create a new connection
    async fn connect(&self, addr: SocketAddr) -> Result<TcpStream, PoolError> {
        let stream = timeout(self.config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| PoolError::Timeout)?
            .map_err(PoolError::Io)?;

        Ok(stream)
    }

    /// Remove expired connections from the pool
    pub async fn cleanup(&self) {
        let mut conns = self.connections.lock().await;
        conns.retain(|_, entries| {
            entries
                .retain(|e| !e.is_expired(self.config.max_age) && !e.is_idle(self.config.max_idle));
            !entries.is_empty()
        });
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let conns = self.connections.lock().await;
        let total_connections = conns.values().map(|v| v.len()).sum();
        PoolStats {
            total_addresses: conns.len(),
            total_connections,
        }
    }
}

/// Connection pool error
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("connection timeout")]
    Timeout,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_addresses: usize,
    pub total_connections: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections_per_addr, 4);
        assert_eq!(config.max_age.as_secs(), 300);
        assert_eq!(config.max_idle.as_secs(), 60);
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
