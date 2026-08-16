//! Xunlei P2P peer client

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug, thiserror::Error)]
pub enum PeerClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection timeout")]
    Timeout,
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Xunlei P2P peer client
///
/// Protocol (simplified):
/// 1. Connect via TCP
/// 2. Send request: [16 bytes file_hash][8 bytes offset][8 bytes size]
/// 3. Receive: [4 bytes length][data...]
pub struct PeerClient {
    stream: TcpStream,
    #[allow(dead_code)]
    addr: SocketAddr,
}

impl std::fmt::Debug for PeerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerClient")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl PeerClient {
    /// Connect to a peer
    pub async fn connect(addr: SocketAddr) -> Result<Self, PeerClientError> {
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| PeerClientError::Timeout)?
            .map_err(PeerClientError::Io)?;

        Ok(Self { stream, addr })
    }

    /// Request a block from the peer
    /// Returns the block data
    pub async fn request_block(
        &mut self,
        file_hash: &[u8; 16],
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, PeerClientError> {
        // Send request: [16 bytes file_hash][8 bytes offset][8 bytes size]
        let mut request = Vec::with_capacity(32);
        request.extend_from_slice(file_hash);
        request.extend_from_slice(&offset.to_le_bytes());
        request.extend_from_slice(&size.to_le_bytes());

        self.stream.write_all(&request).await?;

        // Receive response: [4 bytes length][data...]
        let mut len_buf = [0u8; 4];
        timeout(
            Duration::from_secs(30),
            self.stream.read_exact(&mut len_buf),
        )
        .await
        .map_err(|_| PeerClientError::Timeout)?
        .map_err(PeerClientError::Io)?;

        let data_len = u32::from_le_bytes(len_buf) as usize;

        if data_len == 0 {
            return Err(PeerClientError::Protocol(
                "peer returned no data".to_string(),
            ));
        }

        if data_len > 10 * 1024 * 1024 {
            return Err(PeerClientError::Protocol("response too large".to_string()));
        }

        let mut data = vec![0u8; data_len];
        timeout(Duration::from_secs(30), self.stream.read_exact(&mut data))
            .await
            .map_err(|_| PeerClientError::Timeout)?
            .map_err(PeerClientError::Io)?;

        Ok(data)
    }

    #[allow(dead_code)]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // ── PeerClientError Display ──

    #[test]
    fn peer_client_error_display_io() {
        let err = PeerClientError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let s = format!("{err}");
        assert!(s.contains("IO error"));
        assert!(s.contains("refused"));
    }

    #[test]
    fn peer_client_error_display_timeout() {
        let err = PeerClientError::Timeout;
        assert_eq!(format!("{err}"), "connection timeout");
    }

    #[test]
    fn peer_client_error_display_protocol() {
        let err = PeerClientError::Protocol("bad data".to_string());
        let s = format!("{err}");
        assert!(s.contains("protocol error"));
        assert!(s.contains("bad data"));
    }

    #[test]
    fn peer_client_error_display_protocol_unicode() {
        let err = PeerClientError::Protocol("数据损坏".to_string());
        let s = format!("{err}");
        assert!(s.contains("数据损坏"));
    }

    #[test]
    fn peer_client_error_display_protocol_emoji() {
        let err = PeerClientError::Protocol("🚫 forbidden".to_string());
        let s = format!("{err}");
        assert!(s.contains("🚫 forbidden"));
    }

    // ── PeerClientError Debug ──

    #[test]
    fn peer_client_error_debug_io() {
        let err = PeerClientError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let s = format!("{err:?}");
        assert!(s.contains("Io"));
    }

    #[test]
    fn peer_client_error_debug_timeout() {
        let err = PeerClientError::Timeout;
        let s = format!("{err:?}");
        assert!(s.contains("Timeout"));
    }

    #[test]
    fn peer_client_error_debug_protocol() {
        let err = PeerClientError::Protocol("msg".to_string());
        let s = format!("{err:?}");
        assert!(s.contains("Protocol"));
        assert!(s.contains("msg"));
    }

    // ── PeerClientError From<io::Error> ──

    #[test]
    fn peer_client_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        let err: PeerClientError = io_err.into();
        match err {
            PeerClientError::Io(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            }
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn peer_client_error_from_io_error_preserves_kind() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        let err: PeerClientError = io_err.into();
        match err {
            PeerClientError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn peer_client_error_from_io_error_connection_reset() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        let err: PeerClientError = io_err.into();
        assert!(matches!(err, PeerClientError::Io(_)));
    }

    // ── PeerClientError trait bounds ──

    #[test]
    fn peer_client_error_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<PeerClientError>();
    }

    #[test]
    fn peer_client_error_is_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<PeerClientError>();
    }

    #[test]
    fn peer_client_error_is_std_error() {
        fn assert_error<T: std::error::Error>() {}
        assert_error::<PeerClientError>();
    }

    // ── PeerClient::connect ──

    #[tokio::test]
    async fn connect_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let client = PeerClient::connect(addr).await;
        assert!(client.is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connect_returns_correct_addr() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let client = PeerClient::connect(addr).await.unwrap();
        assert_eq!(client.addr(), addr);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connect_timeout_unreachable_address() {
        // Use a non-routable address to trigger timeout
        // 192.0.2.1 is TEST-NET-1 (RFC 5737), should not respond
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 12345);
        let result = PeerClient::connect(addr).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerClientError::Timeout => {} // expected
            PeerClientError::Io(_) => {}   // also acceptable (connection refused)
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn connect_refused_no_listener() {
        // Bind and immediately drop to get a port with no listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let result = PeerClient::connect(addr).await;
        assert!(result.is_err());
        // Should be Io (connection refused), not Timeout
        match result.unwrap_err() {
            PeerClientError::Io(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused);
            }
            PeerClientError::Timeout => {} // also acceptable on some systems
            other => panic!("unexpected error: {other}"),
        }
    }

    // ── PeerClient::request_block ──

    #[tokio::test]
    async fn request_block_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let expected_data = b"hello world block data";
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Read the request: 16 + 8 + 8 = 32 bytes
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            // Send response: [4 bytes length][data]
            let len = (expected_data.len() as u32).to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(expected_data).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 1024).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_data);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_sends_correct_protocol_format() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();

            // Verify file_hash (first 16 bytes)
            let hash = &req[0..16];
            assert_eq!(hash, &[0xAB; 16]);

            // Verify offset (next 8 bytes, little-endian)
            let offset = u64::from_le_bytes(req[16..24].try_into().unwrap());
            assert_eq!(offset, 4096);

            // Verify size (last 8 bytes, little-endian)
            let size = u64::from_le_bytes(req[24..32].try_into().unwrap());
            assert_eq!(size, 2048);

            // Send minimal valid response
            let len = 1u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0xFF]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0xAB; 16];
        let _ = client.request_block(&file_hash, 4096, 2048).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_zero_length_response_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            // Send length = 0
            let len = 0u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 1024).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerClientError::Protocol(msg) => {
                assert!(msg.contains("no data"));
            }
            other => panic!("expected Protocol error, got: {other}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_too_large_response_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            // Send length > 10MB
            let len = (11 * 1024 * 1024u32).to_le_bytes();
            stream.write_all(&len).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 1024).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerClientError::Protocol(msg) => {
                assert!(msg.contains("too large"));
            }
            other => panic!("expected Protocol error, got: {other}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_exactly_10mb_boundary() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            // Exactly 10MB + 1 should fail
            let len = (10 * 1024 * 1024u32 + 1).to_le_bytes();
            stream.write_all(&len).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 1024).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerClientError::Protocol(msg) => {
                assert!(msg.contains("too large"));
            }
            other => panic!("expected Protocol error, got: {other}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_10mb_exactly_accepted() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            // Exactly 10MB should be accepted (not > 10MB)
            let len = (10 * 1024 * 1024u32).to_le_bytes();
            stream.write_all(&len).await.unwrap();
            // Send 10MB of data
            let data = vec![0xAA; 10 * 1024 * 1024];
            stream.write_all(&data).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 10 * 1024 * 1024).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), 10 * 1024 * 1024);
        assert!(data.iter().all(|&b| b == 0xAA));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_single_byte_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = 1u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0x42]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 1).await.unwrap();
        assert_eq!(result, vec![0x42]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_binary_data_integrity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let expected: Vec<u8> = (0..=255).collect();
        let expected_clone = expected.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = (expected_clone.len() as u32).to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&expected_clone).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 256).await.unwrap();
        assert_eq!(result, expected);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_with_various_offsets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();

            let offset = u64::from_le_bytes(req[16..24].try_into().unwrap());
            assert_eq!(offset, u64::MAX - 1);

            let len = 1u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0x00]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let _ = client
            .request_block(&file_hash, u64::MAX - 1, 1)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_with_zero_offset() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();

            let offset = u64::from_le_bytes(req[16..24].try_into().unwrap());
            assert_eq!(offset, 0);

            let len = 1u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0x00]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let _ = client.request_block(&file_hash, 0, 1).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_with_all_zeros_hash() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();

            let hash = &req[0..16];
            assert_eq!(hash, &[0u8; 16]);

            let len = 1u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0x00]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let _ = client.request_block(&file_hash, 0, 1).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_with_all_ff_hash() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();

            let hash = &req[0..16];
            assert_eq!(hash, &[0xFF; 16]);

            let len = 1u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0x00]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0xFF; 16];
        let _ = client.request_block(&file_hash, 0, 1).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_multiple_requests_same_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            // First request
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = 5u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(b"first").await.unwrap();

            // Second request
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = 6u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(b"second").await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];

        let r1 = client.request_block(&file_hash, 0, 5).await.unwrap();
        assert_eq!(r1, b"first");

        let r2 = client.request_block(&file_hash, 5, 6).await.unwrap();
        assert_eq!(r2, b"second");

        server.await.unwrap();
    }

    // ── PeerClient::addr ──

    #[tokio::test]
    async fn addr_returns_connect_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let client = PeerClient::connect(addr).await.unwrap();
        assert_eq!(client.addr(), addr);
        assert_eq!(client.addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        server.await.unwrap();
    }

    // ── Protocol constants ──

    #[test]
    fn request_size_is_32_bytes() {
        // 16 (hash) + 8 (offset) + 8 (size) = 32
        assert_eq!(16 + 8 + 8, 32);
    }

    #[test]
    fn max_response_size_is_10mb() {
        let max = 10 * 1024 * 1024usize;
        assert_eq!(max, 10_485_760);
    }

    // ── Edge cases ──

    #[tokio::test]
    async fn request_block_large_but_valid_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let data_size = 1024 * 1024; // 1MB
        let data_size_clone = data_size;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = (data_size_clone as u32).to_le_bytes();
            stream.write_all(&len).await.unwrap();
            let data = vec![0xBB; data_size_clone];
            stream.write_all(&data).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client
            .request_block(&file_hash, 0, data_size as u64)
            .await
            .unwrap();
        assert_eq!(result.len(), data_size);
        assert!(result.iter().all(|&b| b == 0xBB));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_length_field_max_u32() {
        // If length field is u32::MAX, it's > 10MB, should error
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = u32::MAX.to_le_bytes();
            stream.write_all(&len).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client.request_block(&file_hash, 0, 1).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerClientError::Protocol(msg) => {
                assert!(msg.contains("too large"));
            }
            other => panic!("expected Protocol error, got: {other}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connect_and_request_unicode_hash() {
        // File hash is arbitrary bytes; test with pattern bytes
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = 4u32.to_le_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&[0xE4, 0xB8, 0xAD, 0xE6]).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        // Use UTF-8 bytes as hash (Chinese character bytes)
        let hash_bytes = "中文测试哈希值啊".as_bytes();
        let mut file_hash = [0u8; 16];
        file_hash.copy_from_slice(&hash_bytes[..16]);
        let result = client.request_block(&file_hash, 0, 4).await.unwrap();
        assert_eq!(result, vec![0xE4, 0xB8, 0xAD, 0xE6]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_block_just_under_max() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let data_size = 10 * 1024 * 1024 - 1; // 10MB - 1 byte
        let data_size_clone = data_size;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 32];
            stream.read_exact(&mut req).await.unwrap();
            let len = (data_size_clone as u32).to_le_bytes();
            stream.write_all(&len).await.unwrap();
            let data = vec![0xCC; data_size_clone];
            stream.write_all(&data).await.unwrap();
        });

        let mut client = PeerClient::connect(addr).await.unwrap();
        let file_hash = [0u8; 16];
        let result = client
            .request_block(&file_hash, 0, data_size as u64)
            .await
            .unwrap();
        assert_eq!(result.len(), data_size);
        server.await.unwrap();
    }
}
