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
    use tokio::net::TcpListener;

    // === Helper: start a local TCP listener and return its address ===
    async fn start_test_server() -> (SocketAddr, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (addr, listener)
    }

    // === SearchType tests ===

    #[test]
    fn test_search_type_repr_values() {
        assert_eq!(SearchType::Local as u32, 1);
        assert_eq!(SearchType::Global as u32, 2);
    }

    #[test]
    fn test_search_type_clone_copy() {
        let st = SearchType::Local;
        let st2 = st; // Copy
        let st3 = st.clone(); // Clone
        assert_eq!(st2 as u32, 1);
        assert_eq!(st3 as u32, 1);
    }

    #[test]
    fn test_search_type_debug() {
        let debug = format!("{:?}", SearchType::Local);
        assert_eq!(debug, "Local");
        let debug = format!("{:?}", SearchType::Global);
        assert_eq!(debug, "Global");
    }

    // === Ed2kClientError tests ===

    #[test]
    fn test_error_display_io() {
        let err = Ed2kClientError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("IO error"));
    }

    #[test]
    fn test_error_display_timeout() {
        let err = Ed2kClientError::Timeout;
        assert_eq!(format!("{}", err), "connection timeout");
    }

    #[test]
    fn test_error_display_protocol() {
        let err = Ed2kClientError::Protocol("bad data".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("protocol error"));
        assert!(msg.contains("bad data"));
    }

    #[test]
    fn test_error_display_proxy() {
        let err = Ed2kClientError::Proxy("proxy failed".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("proxy error"));
        assert!(msg.contains("proxy failed"));
    }

    #[test]
    fn test_error_debug_all_variants() {
        let variants: Vec<Ed2kClientError> = vec![
            Ed2kClientError::Io(std::io::Error::new(std::io::ErrorKind::Other, "x")),
            Ed2kClientError::Timeout,
            Ed2kClientError::Protocol("p".into()),
            Ed2kClientError::Proxy("px".into()),
        ];
        for v in &variants {
            let debug = format!("{:?}", v);
            assert!(!debug.is_empty());
        }
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        let err: Ed2kClientError = Ed2kClientError::from(io_err);
        match err {
            Ed2kClientError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe),
            _ => panic!("Expected Io variant"),
        }
    }

    // === connect tests ===

    #[tokio::test]
    async fn test_connect_success() {
        let (addr, _listener) = start_test_server().await;
        let client = Ed2kClient::connect(addr).await;
        assert!(client.is_ok());
        let c = client.unwrap();
        assert_eq!(c.addr(), addr);
    }

    #[tokio::test]
    async fn test_connect_direct_connection_refused() {
        // Direct connect to a port that shouldn't be open
        let addr: SocketAddr = "127.0.0.1:19999".parse().unwrap();
        let result = Ed2kClient::connect(addr).await;
        assert!(result.is_err());
    }

    // === connect_with_proxy tests ===

    #[tokio::test]
    async fn test_connect_with_http_proxy_returns_error() {
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
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let proxy = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 19999);
        let result = Ed2kClient::connect_with_proxy(addr, &proxy).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_with_auth_connection_refused() {
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

    // === send / recv tests ===

    #[tokio::test]
    async fn test_send_and_recv_roundtrip() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        // Spawn a server task that echoes back a response
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Read the sent message
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();

            // Send response: same protocol, payload = received payload reversed
            let resp_payload: Vec<u8> = payload.into_iter().rev().collect();
            let resp_len = resp_payload.len() as u32;
            let mut resp = Vec::with_capacity(5 + resp_payload.len());
            resp.push(proto);
            resp.extend_from_slice(&resp_len.to_le_bytes());
            resp.extend_from_slice(&resp_payload);
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();
            (proto, len)
        });

        // Send a message
        client.send(0xAB, b"hello").await.unwrap();

        // Receive the echo
        let (resp_proto, resp_payload) = client.recv().await.unwrap();
        assert_eq!(resp_proto, 0xAB);
        assert_eq!(resp_payload, b"olleh"); // reversed "hello"

        let (sent_proto, sent_len) = server.await.unwrap();
        assert_eq!(sent_proto, 0xAB);
        assert_eq!(sent_len, 5);
    }

    #[tokio::test]
    async fn test_send_empty_payload() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            assert_eq!(len, 0);
            proto
        });

        client.send(0x01, &[]).await.unwrap();
        let proto = server.await.unwrap();
        assert_eq!(proto, 0x01);
    }

    #[tokio::test]
    async fn test_send_large_payload() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            len
        });

        let payload = vec![0x42u8; 65536];
        client.send(0x10, &payload).await.unwrap();
        let received_len = server.await.unwrap();
        assert_eq!(received_len, 65536);
    }

    #[tokio::test]
    async fn test_recv_protocol_error_too_large() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Send a message claiming 11MB payload (> 10MB limit)
            let mut header = [0u8; 5];
            header[0] = 0x01; // protocol
            let big_len: u32 = 11 * 1024 * 1024;
            header[1..5].copy_from_slice(&big_len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let result = client.recv().await;
        assert!(result.is_err());
        match result {
            Err(Ed2kClientError::Protocol(msg)) => {
                assert!(msg.contains("too large"));
            }
            _ => panic!("Expected Protocol error for oversized message"),
        }
    }

    #[tokio::test]
    async fn test_recv_exact_10mb_boundary() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Send exactly 10MB — should be accepted (limit is > 10MB)
            let mut header = [0u8; 5];
            header[0] = 0x02;
            let max_len: u32 = 10 * 1024 * 1024;
            header[1..5].copy_from_slice(&max_len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            // Send the actual payload (all zeros)
            let payload = vec![0u8; max_len as usize];
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let result = client.recv().await;
        assert!(result.is_ok());
        let (proto, payload) = result.unwrap();
        assert_eq!(proto, 0x02);
        assert_eq!(payload.len(), 10 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_recv_connection_closed() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            // Drop immediately — client should get an IO error
        });

        server.await.unwrap();
        let result = client.recv().await;
        assert!(result.is_err());
    }

    // === login tests ===

    #[tokio::test]
    async fn test_login_sends_correct_packet() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();

            // Verify protocol byte = 0x01 (LoginRequest)
            assert_eq!(proto, 0x01);

            // First 4 bytes: client_id
            let client_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            assert_eq!(client_id, 0x12345678);

            // Next 2 bytes: port
            let port = u16::from_le_bytes([payload[4], payload[5]]);
            assert_eq!(port, 4662);

            // Next 4 bytes: tag count = 1
            let tag_count = u32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
            assert_eq!(tag_count, 1);

            // Tag: type=0x02 (string), name_len=4, name="name"
            assert_eq!(payload[10], 0x02); // string type
            assert_eq!(payload[11], 0x04); // name length
            assert_eq!(&payload[12..16], b"name");

            // Value: len=5 (u16), "eMule"
            let val_len = u16::from_le_bytes([payload[16], payload[17]]);
            assert_eq!(val_len, 5);
            assert_eq!(&payload[18..23], b"eMule");
        });

        client.login(0x12345678, 4662).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_login_different_client_id_and_port() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();

            let client_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let port = u16::from_le_bytes([payload[4], payload[5]]);
            (client_id, port)
        });

        client.login(0xDEADBEEF, 8080).await.unwrap();
        let (cid, port) = server.await.unwrap();
        assert_eq!(cid, 0xDEADBEEF);
        assert_eq!(port, 8080);
    }

    // === request_sources tests ===

    #[tokio::test]
    async fn test_request_sources_sends_hash() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            (proto, payload)
        });

        let hash = Ed2kFileHash([0xAB; 16]);
        client.request_sources(&hash).await.unwrap();

        let (proto, payload) = server.await.unwrap();
        assert_eq!(proto, 0x19); // GetSources opcode
        assert_eq!(payload.len(), 16);
        assert_eq!(payload, [0xAB; 16]);
    }

    #[tokio::test]
    async fn test_request_sources_all_zeros_hash() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        let hash = Ed2kFileHash([0x00; 16]);
        client.request_sources(&hash).await.unwrap();

        let payload = server.await.unwrap();
        assert_eq!(payload, [0x00; 16]);
    }

    // === search tests ===

    #[tokio::test]
    async fn test_search_local_sends_query() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            (proto, payload)
        });

        client
            .search("test query", SearchType::Local)
            .await
            .unwrap();

        let (proto, payload) = server.await.unwrap();
        assert_eq!(proto, 0x16); // SearchRequest opcode

        // First 4 bytes: search type = 1 (Local)
        let stype = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        assert_eq!(stype, 1);

        // Then: type byte (0x02), name_len (0x00), query_len (u16), query bytes
        assert_eq!(payload[4], 0x02);
        assert_eq!(payload[5], 0x00);
        let qlen = u16::from_le_bytes([payload[6], payload[7]]) as usize;
        assert_eq!(qlen, 10);
        assert_eq!(&payload[8..18], b"test query");
    }

    #[tokio::test]
    async fn test_search_global_type() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        client.search("global", SearchType::Global).await.unwrap();

        let payload = server.await.unwrap();
        let stype = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        assert_eq!(stype, 2); // Global
    }

    #[tokio::test]
    async fn test_search_empty_query() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        client.search("", SearchType::Local).await.unwrap();

        let payload = server.await.unwrap();
        let qlen = u16::from_le_bytes([payload[6], payload[7]]) as usize;
        assert_eq!(qlen, 0);
    }

    #[tokio::test]
    async fn test_search_unicode_query() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        client
            .search("日本語テスト", SearchType::Local)
            .await
            .unwrap();

        let payload = server.await.unwrap();
        let qlen = u16::from_le_bytes([payload[6], payload[7]]) as usize;
        let query_bytes = &payload[8..8 + qlen];
        assert_eq!(std::str::from_utf8(query_bytes).unwrap(), "日本語テスト");
    }

    // === request_server_list tests ===

    #[tokio::test]
    async fn test_request_server_list_empty_payload() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            (proto, len)
        });

        client.request_server_list().await.unwrap();

        let (proto, len) = server.await.unwrap();
        assert_eq!(proto, 0x14); // ServerListRequest
        assert_eq!(len, 0);
    }

    // === request_stats tests ===

    #[tokio::test]
    async fn test_request_stats_empty_payload() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            (proto, len)
        });

        client.request_stats().await.unwrap();

        let (proto, len) = server.await.unwrap();
        assert_eq!(proto, 0x96); // StatGetRequest
        assert_eq!(len, 0);
    }

    // === disconnect tests ===

    #[tokio::test]
    async fn test_disconnect_sends_empty_packet() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            (proto, len)
        });

        client.disconnect().await.unwrap();

        let (proto, len) = server.await.unwrap();
        assert_eq!(proto, 0x05); // Disconnect
        assert_eq!(len, 0);
    }

    // === peer_hello tests ===

    #[tokio::test]
    async fn test_peer_hello_sends_hashes() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            (proto, payload)
        });

        let client_hash = [0xAA; 16];
        let user_hash = [0xBB; 16];
        client.peer_hello(&client_hash, &user_hash).await.unwrap();

        let (proto, payload) = server.await.unwrap();
        assert_eq!(proto, 0x01); // Hello
        // First 16 bytes: client_hash
        assert_eq!(&payload[..16], &[0xAA; 16]);
        // Next 16 bytes: user_hash
        assert_eq!(&payload[16..32], &[0xBB; 16]);
        // Next 4 bytes: tag count = 0
        let tag_count = u32::from_le_bytes([payload[32], payload[33], payload[34], payload[35]]);
        assert_eq!(tag_count, 0);
    }

    #[tokio::test]
    async fn test_peer_hello_all_zeros() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        let client_hash = [0x00; 16];
        let user_hash = [0x00; 16];
        client.peer_hello(&client_hash, &user_hash).await.unwrap();

        let payload = server.await.unwrap();
        assert_eq!(payload.len(), 36); // 16 + 16 + 4
        assert!(payload.iter().all(|&b| b == 0));
    }

    // === request_block tests ===

    #[tokio::test]
    async fn test_request_block_sends_correct_packet() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let proto = header[0];
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            (proto, payload)
        });

        let hash = Ed2kFileHash([0xCC; 16]);
        client.request_block(&hash, 0x1000, 0x4000).await.unwrap();

        let (proto, payload) = server.await.unwrap();
        assert_eq!(proto, 0x52); // StartUploadReq
        // 16 bytes hash + 8 bytes offset + 8 bytes size = 32 bytes
        assert_eq!(payload.len(), 32);
        assert_eq!(&payload[..16], &[0xCC; 16]);
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
        assert_eq!(offset, 0x1000);
        let size = u64::from_le_bytes([
            payload[24],
            payload[25],
            payload[26],
            payload[27],
            payload[28],
            payload[29],
            payload[30],
            payload[31],
        ]);
        assert_eq!(size, 0x4000);
    }

    #[tokio::test]
    async fn test_request_block_zero_offset() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        let hash = Ed2kFileHash([0x11; 16]);
        client.request_block(&hash, 0, 9728000).await.unwrap();

        let payload = server.await.unwrap();
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
        assert_eq!(offset, 0);
        let size = u64::from_le_bytes([
            payload[24],
            payload[25],
            payload[26],
            payload[27],
            payload[28],
            payload[29],
            payload[30],
            payload[31],
        ]);
        assert_eq!(size, 9728000);
    }

    #[tokio::test]
    async fn test_request_block_large_offset() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        let hash = Ed2kFileHash([0x22; 16]);
        let large_offset: u64 = 2 * 1024 * 1024 * 1024; // 2GB
        client
            .request_block(&hash, large_offset, 0x4000)
            .await
            .unwrap();

        let payload = server.await.unwrap();
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
        assert_eq!(offset, large_offset);
    }

    // === receive_block tests ===

    #[tokio::test]
    async fn test_receive_block_success() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Build a FileAnswer: protocol=0x48, payload=[16 hash][8 offset][data]
            let mut payload = Vec::new();
            payload.extend_from_slice(&[0xDD; 16]); // hash
            payload.extend_from_slice(&1024u64.to_le_bytes()); // offset
            payload.extend_from_slice(b"block data here"); // data

            let len = payload.len() as u32;
            let mut header = [0u8; 5];
            header[0] = 0x48; // FileAnswer
            header[1..5].copy_from_slice(&len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let (hash, offset, data) = client.receive_block().await.unwrap();
        assert_eq!(hash, [0xDD; 16]);
        assert_eq!(offset, 1024);
        assert_eq!(data, b"block data here");
    }

    #[tokio::test]
    async fn test_receive_block_wrong_protocol() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut payload = vec![0u8; 32]; // enough data
            let len = payload.len() as u32;
            let mut header = [0u8; 5];
            header[0] = 0x99; // Wrong protocol
            header[1..5].copy_from_slice(&len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let result = client.receive_block().await;
        assert!(result.is_err());
        match result {
            Err(Ed2kClientError::Protocol(msg)) => {
                assert!(msg.contains("unexpected protocol"));
                assert!(msg.contains("0x99"));
            }
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn test_receive_block_payload_too_short() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Send protocol 0x48 but only 20 bytes of payload (< 24 minimum)
            let payload = vec![0u8; 20];
            let len = payload.len() as u32;
            let mut header = [0u8; 5];
            header[0] = 0x48;
            header[1..5].copy_from_slice(&len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let result = client.receive_block().await;
        assert!(result.is_err());
        match result {
            Err(Ed2kClientError::Protocol(msg)) => {
                assert!(msg.contains("invalid file answer"));
            }
            _ => panic!("Expected Protocol error for short payload"),
        }
    }

    #[tokio::test]
    async fn test_receive_block_exact_minimum_payload() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Exactly 24 bytes: 16 hash + 8 offset, no data
            let mut payload = Vec::new();
            payload.extend_from_slice(&[0xEE; 16]);
            payload.extend_from_slice(&0u64.to_le_bytes());
            let len = payload.len() as u32;
            let mut header = [0u8; 5];
            header[0] = 0x48;
            header[1..5].copy_from_slice(&len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let (hash, offset, data) = client.receive_block().await.unwrap();
        assert_eq!(hash, [0xEE; 16]);
        assert_eq!(offset, 0);
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_receive_block_large_data() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut payload = Vec::new();
            payload.extend_from_slice(&[0xFF; 16]);
            payload.extend_from_slice(&4096u64.to_le_bytes());
            payload.extend_from_slice(&vec![0xAB; 4096]);
            let len = payload.len() as u32;
            let mut header = [0u8; 5];
            header[0] = 0x48;
            header[1..5].copy_from_slice(&len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let (hash, offset, data) = client.receive_block().await.unwrap();
        assert_eq!(hash, [0xFF; 16]);
        assert_eq!(offset, 4096);
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0xAB));
    }

    // === addr() tests ===

    #[tokio::test]
    async fn test_addr_returns_connect_address() {
        let (addr, _listener) = start_test_server().await;
        let client = Ed2kClient::connect(addr).await.unwrap();
        assert_eq!(client.addr(), addr);
    }

    #[tokio::test]
    async fn test_addr_ipv4_loopback() {
        let (addr, _listener) = start_test_server().await;
        let client = Ed2kClient::connect(addr).await.unwrap();
        assert!(client.addr().ip().is_loopback());
    }

    // === Multiple operations on same connection ===

    #[tokio::test]
    async fn test_multiple_sends_same_connection() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut protos = Vec::new();
            for _ in 0..3 {
                let mut header = [0u8; 5];
                stream.read_exact(&mut header).await.unwrap();
                protos.push(header[0]);
                let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
                let mut payload = vec![0u8; len];
                stream.read_exact(&mut payload).await.unwrap();
            }
            protos
        });

        client.request_server_list().await.unwrap();
        client.request_stats().await.unwrap();
        client.disconnect().await.unwrap();

        let protos = server.await.unwrap();
        assert_eq!(protos, vec![0x14, 0x96, 0x05]);
    }

    #[tokio::test]
    async fn test_send_recv_multiple_roundtrips() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for i in 0..3u8 {
                let mut header = [0u8; 5];
                stream.read_exact(&mut header).await.unwrap();
                let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
                let mut payload = vec![0u8; len];
                stream.read_exact(&mut payload).await.unwrap();

                // Echo back with protocol = original + 1
                let resp_payload = vec![i; 4];
                let resp_len = resp_payload.len() as u32;
                let mut resp = Vec::new();
                resp.push(header[0] + 1);
                resp.extend_from_slice(&resp_len.to_le_bytes());
                resp.extend_from_slice(&resp_payload);
                stream.write_all(&resp).await.unwrap();
                stream.flush().await.unwrap();
            }
        });

        client.send(0x10, b"aaa").await.unwrap();
        let (p1, d1) = client.recv().await.unwrap();
        assert_eq!(p1, 0x11);
        assert_eq!(d1, vec![0u8; 4]);

        client.send(0x20, b"bbb").await.unwrap();
        let (p2, d2) = client.recv().await.unwrap();
        assert_eq!(p2, 0x21);
        assert_eq!(d2, vec![1u8; 4]);

        client.send(0x30, b"ccc").await.unwrap();
        let (p3, d3) = client.recv().await.unwrap();
        assert_eq!(p3, 0x31);
        assert_eq!(d3, vec![2u8; 4]);

        server.await.unwrap();
    }

    // === Edge cases ===

    #[tokio::test]
    async fn test_recv_zero_length_payload() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Send a message with 0-length payload
            let mut header = [0u8; 5];
            header[0] = 0x42;
            header[1..5].copy_from_slice(&0u32.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let (proto, payload) = client.recv().await.unwrap();
        assert_eq!(proto, 0x42);
        assert!(payload.is_empty());
    }

    #[tokio::test]
    async fn test_send_all_protocol_byte_values() {
        // Verify send works with any protocol byte value
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            header[0]
        });

        client.send(0xFF, &[1, 2, 3]).await.unwrap();
        let proto = server.await.unwrap();
        assert_eq!(proto, 0xFF);
    }

    #[tokio::test]
    async fn test_request_block_u64_max_offset() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();
            payload
        });

        let hash = Ed2kFileHash([0x33; 16]);
        client
            .request_block(&hash, u64::MAX, u64::MAX)
            .await
            .unwrap();

        let payload = server.await.unwrap();
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
        assert_eq!(offset, u64::MAX);
        let size = u64::from_le_bytes([
            payload[24],
            payload[25],
            payload[26],
            payload[27],
            payload[28],
            payload[29],
            payload[30],
            payload[31],
        ]);
        assert_eq!(size, u64::MAX);
    }

    #[tokio::test]
    async fn test_receive_block_hash_preserved() {
        let (addr, listener) = start_test_server().await;
        let mut client = Ed2kClient::connect(addr).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut payload = Vec::new();
            // Use a distinctive hash pattern
            let hash: [u8; 16] = [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
                0x32, 0x10,
            ];
            payload.extend_from_slice(&hash);
            payload.extend_from_slice(&999u64.to_le_bytes());
            payload.extend_from_slice(b"data");
            let len = payload.len() as u32;
            let mut header = [0u8; 5];
            header[0] = 0x48;
            header[1..5].copy_from_slice(&len.to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        server.await.unwrap();
        let (hash, offset, data) = client.receive_block().await.unwrap();
        assert_eq!(
            hash,
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
                0x32, 0x10
            ]
        );
        assert_eq!(offset, 999);
        assert_eq!(data, b"data");
    }
}
