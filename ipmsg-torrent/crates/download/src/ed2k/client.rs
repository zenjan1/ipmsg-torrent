//! eDonkey TCP client for server and peer connections

use super::protocol::Ed2kFileHash;
use crate::proxy::{ProxyConfig, ProxyType};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Trait alias for async stream (read + write + send + sync + unpin)
trait AsyncStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> AsyncStream for T {}

/// Search type for Ed2k search requests
#[derive(Debug, Clone, Copy)]
#[repr(u32)]
#[allow(dead_code)]
pub enum SearchType {
    Local = 1,
    Global = 2,
}

#[derive(Debug, thiserror::Error)]
pub enum Ed2kClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection timeout")]
    Timeout,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("proxy error: {0}")]
    Proxy(String),
}

/// eDonkey client connection
pub struct Ed2kClient {
    stream: Box<dyn AsyncStream>,
    #[allow(dead_code)]
    addr: SocketAddr,
}

impl Ed2kClient {
    /// Connect to an eDonkey server or peer directly (no proxy)
    pub async fn connect(addr: SocketAddr) -> Result<Self, Ed2kClientError> {
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| Ed2kClientError::Timeout)?
            .map_err(Ed2kClientError::Io)?;

        Ok(Self {
            stream: Box::new(stream),
            addr,
        })
    }

    /// Connect to an eDonkey server or peer through a proxy.
    ///
    /// SOCKS5 proxies are supported natively; HTTP CONNECT proxies
    /// are not supported for raw TCP (Ed2k) connections and will
    /// return an error.
    pub async fn connect_with_proxy(
        addr: SocketAddr,
        proxy: &ProxyConfig,
    ) -> Result<Self, Ed2kClientError> {
        let stream: Box<dyn AsyncStream> = match proxy.proxy_type {
            ProxyType::Socks5 => {
                let proxy_addr = format!("{}:{}", proxy.host, proxy.port);
                let target = tokio_socks::TargetAddr::Ip(addr);

                let socks_stream = if let Some(ref auth) = proxy.auth {
                    let fut = tokio_socks::tcp::Socks5Stream::connect_with_password(
                        proxy_addr.as_str(),
                        target,
                        auth.username.as_str(),
                        auth.password.as_str(),
                    );
                    timeout(Duration::from_secs(15), fut)
                        .await
                        .map_err(|_| Ed2kClientError::Timeout)?
                        .map_err(|e| Ed2kClientError::Proxy(e.to_string()))?
                } else {
                    let fut = tokio_socks::tcp::Socks5Stream::connect(proxy_addr.as_str(), target);
                    timeout(Duration::from_secs(15), fut)
                        .await
                        .map_err(|_| Ed2kClientError::Timeout)?
                        .map_err(|e| Ed2kClientError::Proxy(e.to_string()))?
                };

                Box::new(socks_stream)
            }
            ProxyType::Http => {
                return Err(Ed2kClientError::Proxy(
                    "HTTP CONNECT proxies are not supported for Ed2k connections".to_string(),
                ));
            }
        };

        Ok(Self { stream, addr })
    }

    /// Send a message to the server/peer
    /// Format: [1 byte protocol][4 bytes length][payload]
    pub async fn send(&mut self, protocol: u8, payload: &[u8]) -> Result<(), Ed2kClientError> {
        let len = payload.len() as u32;
        let mut buf = Vec::with_capacity(5 + payload.len());
        buf.push(protocol);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(payload);

        self.stream.write_all(&buf).await?;
        Ok(())
    }

    /// Receive a message from the server/peer
    pub async fn recv(&mut self) -> Result<(u8, Vec<u8>), Ed2kClientError> {
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header).await?;

        let protocol = header[0];
        let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

        if len > 10 * 1024 * 1024 {
            // Sanity check: reject messages > 10MB
            return Err(Ed2kClientError::Protocol("message too large".to_string()));
        }

        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await?;

        Ok((protocol, payload))
    }

    /// Send login request to server
    pub async fn login(&mut self, client_id: u32, port: u16) -> Result<(), Ed2kClientError> {
        // Login packet: [4 bytes client ID][2 bytes port][4 bytes tag count]...
        let mut payload = Vec::new();
        payload.extend_from_slice(&client_id.to_le_bytes());
        payload.extend_from_slice(&port.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes()); // 1 tag

        // Tag: name="eMule", value="0.50a"
        // Tag format: [1 byte type][1 byte name len][name][value]
        payload.push(0x02); // String type
        payload.push(0x04); // Name length
        payload.extend_from_slice(b"name");
        let value = "eMule";
        payload.extend_from_slice(&(value.len() as u16).to_le_bytes());
        payload.extend_from_slice(value.as_bytes());

        self.send(0x01, &payload).await?; // 0x01 = LoginRequest
        Ok(())
    }

    /// Request sources for a file from server
    pub async fn request_sources(
        &mut self,
        file_hash: &Ed2kFileHash,
    ) -> Result<(), Ed2kClientError> {
        // GetSources packet: [16 bytes file hash]
        self.send(0x19, &file_hash.0).await?;
        Ok(())
    }

    /// Search for files on server
    #[allow(dead_code)]
    pub async fn search(
        &mut self,
        query: &str,
        search_type: SearchType,
    ) -> Result<(), Ed2kClientError> {
        // Search packet: [4 bytes search type][search data...]
        let mut payload = Vec::new();
        payload.extend_from_slice(&(search_type as u32).to_le_bytes());

        // Search metadata: type=STRING, name=""
        payload.push(0x02); // String type
        payload.push(0x00); // Empty name
        payload.extend_from_slice(&(query.len() as u16).to_le_bytes());
        payload.extend_from_slice(query.as_bytes());

        self.send(0x16, &payload).await?; // 0x16 = SearchRequest
        Ok(())
    }

    /// Request server list from current server
    #[allow(dead_code)]
    pub async fn request_server_list(&mut self) -> Result<(), Ed2kClientError> {
        // ServerListRequest: empty payload
        self.send(0x14, &[]).await?; // 0x14 = ServerListRequest
        Ok(())
    }

    /// Request server statistics
    #[allow(dead_code)]
    pub async fn request_stats(&mut self) -> Result<(), Ed2kClientError> {
        // StatGetRequest: empty payload
        self.send(0x96, &[]).await?; // 0x96 = StatGetRequest
        Ok(())
    }

    /// Disconnect from server
    #[allow(dead_code)]
    pub async fn disconnect(&mut self) -> Result<(), Ed2kClientError> {
        // Disconnect: empty payload
        self.send(0x05, &[]).await?; // 0x05 = Disconnect
        Ok(())
    }

    /// Send hello to peer
    #[allow(dead_code)]
    pub async fn peer_hello(
        &mut self,
        client_hash: &[u8; 16],
        user_hash: &[u8; 16],
    ) -> Result<(), Ed2kClientError> {
        // Hello packet: [16 bytes client hash][16 bytes user hash]...
        let mut payload = Vec::new();
        payload.extend_from_slice(client_hash);
        payload.extend_from_slice(user_hash);
        // Simplified: real implementation needs more tags
        payload.extend_from_slice(&0u32.to_le_bytes()); // 0 tags for now

        self.send(0x01, &payload).await?; // 0x01 = Hello
        Ok(())
    }

    /// Request a block from peer
    pub async fn request_block(
        &mut self,
        file_hash: &Ed2kFileHash,
        offset: u64,
        size: u64,
    ) -> Result<(), Ed2kClientError> {
        // Request packet: [16 bytes file hash][8 bytes offset][8 bytes size]
        let mut payload = Vec::new();
        payload.extend_from_slice(&file_hash.0);
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(&size.to_le_bytes());

        self.send(0x52, &payload).await?; // 0x52 = StartUploadReq (simplified)
        Ok(())
    }

    /// Receive a file block from peer
    /// Returns (file_hash, offset, data)
    pub async fn receive_block(&mut self) -> Result<([u8; 16], u64, Vec<u8>), Ed2kClientError> {
        let (protocol, payload) = self.recv().await?;

        if protocol != 0x48 {
            // 0x48 = FileAnswer
            return Err(Ed2kClientError::Protocol(format!(
                "unexpected protocol: 0x{:02x}",
                protocol
            )));
        }

        // FileAnswer: [16 bytes hash][8 bytes offset][data...]
        if payload.len() < 24 {
            return Err(Ed2kClientError::Protocol("invalid file answer".to_string()));
        }

        let mut hash = [0u8; 16];
        hash.copy_from_slice(&payload[..16]);
        let offset = u64::from_le_bytes([
            payload[16],
            payload[17],
            payload[18],
            payload[19],
            payload[20],
            payload[21],
            payload[22],
            payload[23],
        ]);
        let data = payload[24..].to_vec();

        Ok((hash, offset, data))
    }

    #[allow(dead_code)]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{ProxyConfig, ProxyType};

    #[tokio::test]
    async fn test_connect_with_http_proxy_returns_error() {
        // HTTP CONNECT proxies are not supported for Ed2k TCP connections
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let proxy = ProxyConfig::new(ProxyType::Http, "127.0.0.1".into(), 8080);
        let result = Ed2kClient::connect_with_proxy(addr, &proxy).await;
        assert!(result.is_err());
        match result {
            Err(Ed2kClientError::Proxy(msg)) => {
                assert!(msg.contains("HTTP CONNECT"));
            }
            _ => panic!("Expected Proxy error"),
        }
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_connection_refused() {
        // SOCKS5 proxy that doesn't exist — should get a proxy/IO error, not a panic
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let proxy = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 19999);
        let result = Ed2kClient::connect_with_proxy(addr, &proxy).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_with_auth_connection_refused() {
        // SOCKS5 proxy with auth credentials — connection refused expected
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let proxy = ProxyConfig::with_auth(
            ProxyType::Socks5,
            "127.0.0.1".into(),
            19999,
            "user".into(),
            "pass".into(),
        );
        let result = Ed2kClient::connect_with_proxy(addr, &proxy).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_direct_connection_refused() {
        // Direct connect to a port that shouldn't be open
        let addr: SocketAddr = "127.0.0.1:19999".parse().unwrap();
        let result = Ed2kClient::connect(addr).await;
        assert!(result.is_err());
    }
}
