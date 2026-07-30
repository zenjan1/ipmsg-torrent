//! eDonkey TCP client for server and peer connections

use super::protocol::{ED2K_BLOCK_SIZE, Ed2kFileHash};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug, thiserror::Error)]
pub enum Ed2kClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection timeout")]
    Timeout,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("disconnected")]
    Disconnected,
}

/// eDonkey client connection
pub struct Ed2kClient {
    stream: TcpStream,
    addr: SocketAddr,
}

impl Ed2kClient {
    /// Connect to an eDonkey server or peer
    pub async fn connect(addr: SocketAddr) -> Result<Self, Ed2kClientError> {
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| Ed2kClientError::Timeout)?
            .map_err(Ed2kClientError::Io)?;

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

    /// Send hello to peer
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
    pub async fn receive_block(&mut self) -> Result<Vec<u8>, Ed2kClientError> {
        let (protocol, payload) = self.recv().await?;

        if protocol != 0x48 {
            // 0x48 = FileAnswer
            return Err(Ed2kClientError::Protocol(format!(
                "unexpected protocol: 0x{:02x}",
                protocol
            )));
        }

        // FileAnswer: [16 bytes hash][data...]
        if payload.len() < 16 {
            return Err(Ed2kClientError::Protocol("invalid file answer".to_string()));
        }

        Ok(payload[16..].to_vec())
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
