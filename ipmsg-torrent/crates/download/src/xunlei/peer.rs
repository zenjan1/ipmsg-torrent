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
    addr: SocketAddr,
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

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
