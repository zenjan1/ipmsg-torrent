//! BitTorrent peer protocol implementation

use crate::proxy::{ProxyConfig, ProxyType};
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// BitTorrent protocol identifier
pub const BITTORRENT_PROTOCOL: &[u8] = b"BitTorrent protocol";

/// Peer message types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        data: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    KeepAlive,
    Port(u16), // DHT port
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("timeout")]
    Timeout,
    #[error("peer disconnected")]
    #[allow(dead_code)]
    Disconnected,
    #[error("proxy error: {0}")]
    Proxy(String),
}

/// A stream that implements both AsyncRead and AsyncWrite.
trait ReadWriteStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> ReadWriteStream for T {}

/// Type alias for the underlying stream (direct TCP or proxied).
type PeerStream = Box<dyn ReadWriteStream>;

/// BitTorrent peer connection
pub struct PeerConnection {
    stream: PeerStream,
    #[allow(dead_code)]
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    #[allow(dead_code)]
    am_choking: bool,
    #[allow(dead_code)]
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    peer_bitfield: Vec<u8>,
}

impl PeerConnection {
    /// Connect to a peer directly (no proxy) and perform handshake
    pub async fn connect(
        addr: std::net::SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<Self, PeerError> {
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| PeerError::Timeout)?
            .map_err(PeerError::Io)?;

        let mut conn = Self {
            stream: Box::new(stream),
            peer_id: [0u8; 20],
            info_hash,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield: Vec::new(),
        };

        // Perform handshake
        conn.handshake(peer_id).await?;

        Ok(conn)
    }

    /// Connect to a peer through a proxy and perform handshake.
    ///
    /// SOCKS5 proxies are supported natively; HTTP CONNECT proxies
    /// are not supported for raw TCP (BitTorrent) connections and
    /// will return an error.
    pub async fn connect_with_proxy(
        addr: std::net::SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        proxy: &ProxyConfig,
    ) -> Result<Self, PeerError> {
        let stream = match proxy.proxy_type {
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
                        .map_err(|_| PeerError::Timeout)?
                        .map_err(|e| PeerError::Proxy(e.to_string()))?
                } else {
                    let fut = tokio_socks::tcp::Socks5Stream::connect(proxy_addr.as_str(), target);
                    timeout(Duration::from_secs(15), fut)
                        .await
                        .map_err(|_| PeerError::Timeout)?
                        .map_err(|e| PeerError::Proxy(e.to_string()))?
                };

                Box::new(socks_stream) as PeerStream
            }
            ProxyType::Http => {
                return Err(PeerError::Proxy(
                    "HTTP CONNECT proxies are not supported for BitTorrent peer connections"
                        .to_string(),
                ));
            }
        };

        let mut conn = Self {
            stream,
            peer_id: [0u8; 20],
            info_hash,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield: Vec::new(),
        };

        conn.handshake(peer_id).await?;

        Ok(conn)
    }

    async fn handshake(&mut self, my_peer_id: [u8; 20]) -> Result<(), PeerError> {
        // Send handshake
        let mut handshake = Vec::with_capacity(68);
        handshake.push(BITTORRENT_PROTOCOL.len() as u8);
        handshake.extend_from_slice(BITTORRENT_PROTOCOL);
        handshake.extend_from_slice(&[0u8; 8]); // Reserved bytes
        handshake.extend_from_slice(&self.info_hash);
        handshake.extend_from_slice(&my_peer_id);

        self.stream.write_all(&handshake).await?;

        // Receive handshake
        let mut response = vec![0u8; 68];
        timeout(
            Duration::from_secs(10),
            self.stream.read_exact(&mut response),
        )
        .await
        .map_err(|_| PeerError::Timeout)?
        .map_err(PeerError::Io)?;

        // Validate protocol
        let pstrlen = response[0] as usize;
        if pstrlen != 19 {
            return Err(PeerError::Protocol(
                "invalid protocol string length".to_string(),
            ));
        }

        let protocol = &response[1..20];
        if protocol != BITTORRENT_PROTOCOL {
            return Err(PeerError::Protocol("invalid protocol string".to_string()));
        }

        // Validate info hash
        let received_info_hash = &response[28..48];
        if received_info_hash != self.info_hash {
            return Err(PeerError::Protocol("info hash mismatch".to_string()));
        }

        // Store peer ID
        self.peer_id.copy_from_slice(&response[48..68]);

        Ok(())
    }

    /// Send a message to the peer
    pub async fn send(&mut self, msg: PeerMessage) -> Result<(), PeerError> {
        let data = self.encode_message(&msg);
        self.stream.write_all(&data).await?;
        Ok(())
    }

    /// Send raw bytes to the peer (for extended protocol messages)
    pub async fn send_raw(&mut self, data: &[u8]) -> Result<(), PeerError> {
        // Prefix with length
        let len_bytes = (data.len() as u32).to_be_bytes();
        self.stream.write_all(&len_bytes).await?;
        self.stream.write_all(data).await?;
        Ok(())
    }

    /// Read exact bytes from the peer (for extended protocol messages)
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), PeerError> {
        timeout(Duration::from_secs(30), self.stream.read_exact(buf))
            .await
            .map_err(|_| PeerError::Timeout)?
            .map_err(PeerError::Io)?;
        Ok(())
    }

    /// Receive a message from the peer
    pub async fn recv(&mut self) -> Result<PeerMessage, PeerError> {
        // Read message length (4 bytes)
        let mut len_buf = [0u8; 4];
        timeout(
            Duration::from_secs(30),
            self.stream.read_exact(&mut len_buf),
        )
        .await
        .map_err(|_| PeerError::Timeout)?
        .map_err(PeerError::Io)?;

        let msg_len = u32::from_be_bytes(len_buf) as usize;

        // Keep-alive
        if msg_len == 0 {
            return Ok(PeerMessage::KeepAlive);
        }

        // Read message payload
        let mut payload = vec![0u8; msg_len];
        timeout(
            Duration::from_secs(30),
            self.stream.read_exact(&mut payload),
        )
        .await
        .map_err(|_| PeerError::Timeout)?
        .map_err(PeerError::Io)?;

        self.decode_message(&payload)
    }

    fn encode_message(&self, msg: &PeerMessage) -> Vec<u8> {
        match msg {
            PeerMessage::Choke => vec![0, 0, 0, 1, 0],
            PeerMessage::Unchoke => vec![0, 0, 0, 1, 1],
            PeerMessage::Interested => vec![0, 0, 0, 1, 2],
            PeerMessage::NotInterested => vec![0, 0, 0, 1, 3],
            PeerMessage::Have { piece_index } => {
                let mut data = vec![0, 0, 0, 5, 4];
                data.extend_from_slice(&piece_index.to_be_bytes());
                data
            }
            PeerMessage::Bitfield(bitfield) => {
                let mut data = vec![0, 0, 0, 0, 5];
                let len = 1 + bitfield.len();
                data[0..4].copy_from_slice(&(len as u32).to_be_bytes());
                data.extend_from_slice(bitfield);
                data
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let mut data = vec![0, 0, 0, 13, 6];
                data.extend_from_slice(&index.to_be_bytes());
                data.extend_from_slice(&begin.to_be_bytes());
                data.extend_from_slice(&length.to_be_bytes());
                data
            }
            PeerMessage::Piece {
                index,
                begin,
                data: block,
            } => {
                let len = 9 + block.len();
                let mut result = Vec::with_capacity(4 + len);
                result.extend_from_slice(&(len as u32).to_be_bytes());
                result.push(7);
                result.extend_from_slice(&index.to_be_bytes());
                result.extend_from_slice(&begin.to_be_bytes());
                result.extend_from_slice(block);
                result
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                let mut data = vec![0, 0, 0, 13, 8];
                data.extend_from_slice(&index.to_be_bytes());
                data.extend_from_slice(&begin.to_be_bytes());
                data.extend_from_slice(&length.to_be_bytes());
                data
            }
            PeerMessage::KeepAlive => vec![0, 0, 0, 0],
            PeerMessage::Port(port) => {
                let mut data = vec![0, 0, 0, 3, 9];
                data.extend_from_slice(&port.to_be_bytes());
                data
            }
        }
    }

    fn decode_message(&mut self, payload: &[u8]) -> Result<PeerMessage, PeerError> {
        if payload.is_empty() {
            return Err(PeerError::Protocol("empty message".to_string()));
        }

        let msg_id = payload[0];
        let data = &payload[1..];

        match msg_id {
            0 => Ok(PeerMessage::Choke),
            1 => Ok(PeerMessage::Unchoke),
            2 => Ok(PeerMessage::Interested),
            3 => Ok(PeerMessage::NotInterested),
            4 => {
                if data.len() < 4 {
                    return Err(PeerError::Protocol("invalid have message".to_string()));
                }
                let piece_index = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(PeerMessage::Have { piece_index })
            }
            5 => {
                self.peer_bitfield = data.to_vec();
                Ok(PeerMessage::Bitfield(data.to_vec()))
            }
            6 => {
                if data.len() < 12 {
                    return Err(PeerError::Protocol("invalid request message".to_string()));
                }
                let index = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let begin = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
                let length = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
                Ok(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })
            }
            7 => {
                if data.len() < 8 {
                    return Err(PeerError::Protocol("invalid piece message".to_string()));
                }
                let index = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let begin = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
                let block = data[8..].to_vec();
                Ok(PeerMessage::Piece {
                    index,
                    begin,
                    data: block,
                })
            }
            8 => {
                if data.len() < 12 {
                    return Err(PeerError::Protocol("invalid cancel message".to_string()));
                }
                let index = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let begin = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
                let length = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
                Ok(PeerMessage::Cancel {
                    index,
                    begin,
                    length,
                })
            }
            9 => {
                if data.len() < 2 {
                    return Err(PeerError::Protocol("invalid port message".to_string()));
                }
                let port = u16::from_be_bytes([data[0], data[1]]);
                Ok(PeerMessage::Port(port))
            }
            _ => Err(PeerError::Protocol(format!(
                "unknown message id: {}",
                msg_id
            ))),
        }
    }

    /// Update peer state based on received message
    pub fn update_state(&mut self, msg: &PeerMessage) {
        match msg {
            PeerMessage::Choke => self.peer_choking = true,
            PeerMessage::Unchoke => self.peer_choking = false,
            PeerMessage::Interested => self.peer_interested = true,
            PeerMessage::NotInterested => self.peer_interested = false,
            _ => {}
        }
    }

    #[allow(dead_code)]
    pub fn peer_id(&self) -> &[u8; 20] {
        &self.peer_id
    }

    pub fn is_choking(&self) -> bool {
        self.peer_choking
    }

    #[allow(dead_code)]
    pub fn is_interested(&self) -> bool {
        self.peer_interested
    }

    #[allow(dead_code)]
    pub fn has_piece(&self, piece_index: u32) -> bool {
        if self.peer_bitfield.is_empty() {
            return false;
        }
        let byte_index = (piece_index / 8) as usize;
        let bit_index = 7 - (piece_index % 8);
        if byte_index < self.peer_bitfield.len() {
            (self.peer_bitfield[byte_index] & (1 << bit_index)) != 0
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{ProxyConfig, ProxyType};
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_connect_with_http_proxy_rejected() {
        // HTTP CONNECT proxies should be rejected for BitTorrent peer connections
        let proxy = ProxyConfig::new(ProxyType::Http, "127.0.0.1".into(), 8080);
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let info_hash = [0u8; 20];
        let peer_id = [0u8; 20];

        let result = PeerConnection::connect_with_proxy(addr, info_hash, peer_id, &proxy).await;
        assert!(result.is_err());
        if let Err(e) = result {
            match e {
                PeerError::Proxy(msg) => {
                    assert!(msg.contains("HTTP CONNECT"));
                }
                _ => panic!("Expected PeerError::Proxy"),
            }
        }
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_timeout() {
        // SOCKS5 proxy connection should timeout if proxy is unreachable
        let proxy = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 9999);
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let info_hash = [0u8; 20];
        let peer_id = [0u8; 20];

        let result = PeerConnection::connect_with_proxy(addr, info_hash, peer_id, &proxy).await;
        assert!(result.is_err());
        // Should be either Timeout or Proxy error depending on system state
        if let Err(e) = result {
            match e {
                PeerError::Timeout | PeerError::Proxy(_) => {}
                _ => panic!("Expected Timeout or Proxy error"),
            }
        }
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_with_auth() {
        // SOCKS5 proxy with authentication should attempt connection
        let proxy = ProxyConfig::with_auth(
            ProxyType::Socks5,
            "127.0.0.1".into(),
            9999,
            "user".into(),
            "pass".into(),
        );
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let info_hash = [0u8; 20];
        let peer_id = [0u8; 20];

        let result = PeerConnection::connect_with_proxy(addr, info_hash, peer_id, &proxy).await;
        assert!(result.is_err());
        // Should fail to connect (no proxy running), but should not panic
        if let Err(e) = result {
            match e {
                PeerError::Timeout | PeerError::Proxy(_) | PeerError::Io(_) => {}
                _ => panic!("Expected connection error"),
            }
        }
    }
}
