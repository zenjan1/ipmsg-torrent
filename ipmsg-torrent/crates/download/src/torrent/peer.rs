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

    /// Helper: create a PeerConnection with a dummy in-memory stream for testing.
    async fn make_test_connection() -> PeerConnection {
        let (client, _server) = tokio::io::duplex(65536);
        PeerConnection {
            stream: Box::new(client),
            peer_id: [0u8; 20],
            info_hash: [0xAA; 20],
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield: Vec::new(),
        }
    }

    // ===== Constants =====

    #[test]
    fn bittorrent_protocol_value() {
        assert_eq!(BITTORRENT_PROTOCOL, b"BitTorrent protocol");
    }

    #[test]
    fn bittorrent_protocol_length() {
        assert_eq!(BITTORRENT_PROTOCOL.len(), 19);
    }

    // ===== PeerMessage Clone/Debug/PartialEq =====

    #[test]
    fn peer_message_clone_all_variants() {
        let messages = vec![
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::Have { piece_index: 42 },
            PeerMessage::Bitfield(vec![0xFF, 0x00]),
            PeerMessage::Request {
                index: 1,
                begin: 2,
                length: 3,
            },
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                data: vec![1, 2, 3],
            },
            PeerMessage::Cancel {
                index: 5,
                begin: 10,
                length: 15,
            },
            PeerMessage::KeepAlive,
            PeerMessage::Port(6881),
        ];
        for msg in &messages {
            assert_eq!(msg.clone(), *msg);
        }
    }

    #[test]
    fn peer_message_clone_bitfield_independence() {
        let msg = PeerMessage::Bitfield(vec![0xFF, 0x00]);
        let mut cloned = msg.clone();
        if let PeerMessage::Bitfield(ref mut bf) = cloned {
            bf[0] = 0x00;
        }
        assert_ne!(cloned, msg);
    }

    #[test]
    fn peer_message_clone_piece_data_independence() {
        let msg = PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: vec![10, 20],
        };
        let mut cloned = msg.clone();
        if let PeerMessage::Piece { ref mut data, .. } = cloned {
            data[0] = 99;
        }
        assert_ne!(cloned, msg);
    }

    #[test]
    fn peer_message_debug_format() {
        let dbg = format!("{:?}", PeerMessage::Choke);
        assert!(dbg.contains("Choke"));
    }

    #[test]
    fn peer_message_debug_have() {
        let dbg = format!("{:?}", PeerMessage::Have { piece_index: 7 });
        assert!(dbg.contains("Have"));
        assert!(dbg.contains("7"));
    }

    #[test]
    fn peer_message_eq_different_variants() {
        assert_ne!(PeerMessage::Choke, PeerMessage::Unchoke);
        assert_ne!(PeerMessage::Interested, PeerMessage::NotInterested);
        assert_ne!(PeerMessage::KeepAlive, PeerMessage::Port(0));
    }

    #[test]
    fn peer_message_eq_have_different_index() {
        assert_ne!(
            PeerMessage::Have { piece_index: 0 },
            PeerMessage::Have { piece_index: 1 }
        );
    }

    #[test]
    fn peer_message_eq_request_different_fields() {
        let a = PeerMessage::Request {
            index: 0,
            begin: 0,
            length: 0,
        };
        let b = PeerMessage::Request {
            index: 1,
            begin: 0,
            length: 0,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn peer_message_eq_cancel_different() {
        let a = PeerMessage::Cancel {
            index: 0,
            begin: 0,
            length: 0,
        };
        let b = PeerMessage::Cancel {
            index: 0,
            begin: 0,
            length: 1,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn peer_message_eq_port_different() {
        assert_ne!(PeerMessage::Port(0), PeerMessage::Port(65535));
    }

    #[test]
    fn peer_message_eq_bitfield_different() {
        assert_ne!(
            PeerMessage::Bitfield(vec![0xFF]),
            PeerMessage::Bitfield(vec![0x00])
        );
    }

    #[test]
    fn peer_message_eq_bitfield_different_length() {
        assert_ne!(
            PeerMessage::Bitfield(vec![0xFF]),
            PeerMessage::Bitfield(vec![0xFF, 0x00])
        );
    }

    #[test]
    fn peer_message_have_boundary_values() {
        let zero = PeerMessage::Have { piece_index: 0 };
        assert_eq!(zero.clone(), zero);
        let max = PeerMessage::Have {
            piece_index: u32::MAX,
        };
        assert_eq!(max.clone(), max);
    }

    #[test]
    fn peer_message_port_boundary_values() {
        let zero = PeerMessage::Port(0);
        assert_eq!(zero.clone(), zero);
        let max = PeerMessage::Port(u16::MAX);
        assert_eq!(max.clone(), max);
    }

    #[test]
    fn peer_message_request_max_values() {
        let msg = PeerMessage::Request {
            index: u32::MAX,
            begin: u32::MAX,
            length: u32::MAX,
        };
        assert_eq!(msg.clone(), msg);
    }

    #[test]
    fn peer_message_piece_empty_data() {
        let msg = PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: vec![],
        };
        assert_eq!(msg.clone(), msg);
    }

    #[test]
    fn peer_message_piece_large_data() {
        let data = vec![0xAB; 16384];
        let msg = PeerMessage::Piece {
            index: 100,
            begin: 0,
            data: data.clone(),
        };
        let cloned = msg.clone();
        assert_eq!(cloned, msg);
        if let PeerMessage::Piece { data: d, .. } = cloned {
            assert_eq!(d.len(), 16384);
        }
    }

    #[test]
    fn peer_message_bitfield_empty() {
        let msg = PeerMessage::Bitfield(vec![]);
        assert_eq!(msg.clone(), msg);
    }

    #[test]
    fn peer_message_large_bitfield() {
        let bf = vec![0xFF; 1000];
        let msg = PeerMessage::Bitfield(bf.clone());
        let cloned = msg.clone();
        assert_eq!(cloned, msg);
    }

    #[test]
    fn peer_message_piece_unicode_data() {
        let data = "你好世界".as_bytes().to_vec();
        let msg = PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: data.clone(),
        };
        let cloned = msg.clone();
        if let PeerMessage::Piece { data: d, .. } = cloned {
            assert_eq!(d, data);
        }
    }

    #[test]
    fn peer_message_all_variants_distinct() {
        let messages = vec![
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::Have { piece_index: 0 },
            PeerMessage::Bitfield(vec![]),
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 0,
            },
            PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![],
            },
            PeerMessage::Cancel {
                index: 0,
                begin: 0,
                length: 0,
            },
            PeerMessage::KeepAlive,
            PeerMessage::Port(0),
        ];
        for i in 0..messages.len() {
            for j in (i + 1)..messages.len() {
                assert_ne!(messages[i], messages[j]);
            }
        }
    }

    // ===== PeerError Display =====

    #[test]
    fn peer_error_io_display() {
        let err = PeerError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "pipe broken"));
        let msg = format!("{}", err);
        assert!(msg.contains("IO error"));
    }

    #[test]
    fn peer_error_protocol_display() {
        let err = PeerError::Protocol("bad handshake".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("protocol error"));
        assert!(msg.contains("bad handshake"));
    }

    #[test]
    fn peer_error_timeout_display() {
        let err = PeerError::Timeout;
        assert!(format!("{}", err).contains("timeout"));
    }

    #[test]
    fn peer_error_disconnected_display() {
        let err = PeerError::Disconnected;
        assert!(format!("{}", err).contains("peer disconnected"));
    }

    #[test]
    fn peer_error_proxy_display() {
        let err = PeerError::Proxy("SOCKS refused".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("proxy error"));
        assert!(msg.contains("SOCKS refused"));
    }

    // ===== PeerError Debug =====

    #[test]
    fn peer_error_debug_variants() {
        assert!(
            format!(
                "{:?}",
                PeerError::Io(io::Error::new(io::ErrorKind::Other, "x"))
            )
            .contains("Io")
        );
        assert!(format!("{:?}", PeerError::Protocol("x".into())).contains("Protocol"));
        assert!(format!("{:?}", PeerError::Timeout).contains("Timeout"));
        assert!(format!("{:?}", PeerError::Disconnected).contains("Disconnected"));
        assert!(format!("{:?}", PeerError::Proxy("x".into())).contains("Proxy"));
    }

    // ===== PeerError From<io::Error> =====

    #[test]
    fn peer_error_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionReset, "reset");
        let peer_err: PeerError = PeerError::from(io_err);
        match peer_err {
            PeerError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::ConnectionReset),
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn peer_error_from_io_preserves_message() {
        let io_err = io::Error::new(io::ErrorKind::UnexpectedEof, "eof");
        let peer_err: PeerError = PeerError::from(io_err);
        assert!(format!("{}", peer_err).contains("eof"));
    }

    #[test]
    fn peer_error_question_mark_operator() {
        fn may_fail() -> Result<(), PeerError> {
            let _f = std::fs::File::open("/nonexistent_path_12345")?;
            Ok(())
        }
        let result = may_fail();
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerError::Io(_) => {}
            _ => panic!("Expected Io variant from ? operator"),
        }
    }

    // ===== PeerError std::error::Error =====

    #[test]
    fn peer_error_io_has_source() {
        use std::error::Error;
        let err = PeerError::Io(io::Error::new(io::ErrorKind::Other, "inner"));
        assert!(err.source().is_some());
    }

    #[test]
    fn peer_error_others_no_source() {
        use std::error::Error;
        assert!(PeerError::Protocol("x".into()).source().is_none());
        assert!(PeerError::Timeout.source().is_none());
        assert!(PeerError::Disconnected.source().is_none());
        assert!(PeerError::Proxy("x".into()).source().is_none());
    }

    // ===== PeerError Unicode =====

    #[test]
    fn peer_error_protocol_unicode() {
        let err = PeerError::Protocol("协议错误：握手失败".to_string());
        assert!(format!("{}", err).contains("协议错误"));
    }

    #[test]
    fn peer_error_proxy_unicode() {
        let err = PeerError::Proxy("代理连接失败".to_string());
        assert!(format!("{}", err).contains("代理连接失败"));
    }

    #[test]
    fn peer_error_protocol_emoji() {
        let err = PeerError::Protocol("🚫 invalid handshake".to_string());
        assert!(format!("{}", err).contains("🚫"));
    }

    #[test]
    fn peer_error_empty_messages() {
        let err1 = PeerError::Protocol(String::new());
        assert!(format!("{}", err1).contains("protocol error"));
        let err2 = PeerError::Proxy(String::new());
        assert!(format!("{}", err2).contains("proxy error"));
    }

    // ===== encode_message =====

    #[tokio::test]
    async fn encode_choke() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Choke),
            vec![0, 0, 0, 1, 0]
        );
    }

    #[tokio::test]
    async fn encode_unchoke() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Unchoke),
            vec![0, 0, 0, 1, 1]
        );
    }

    #[tokio::test]
    async fn encode_interested() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Interested),
            vec![0, 0, 0, 1, 2]
        );
    }

    #[tokio::test]
    async fn encode_not_interested() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::NotInterested),
            vec![0, 0, 0, 1, 3]
        );
    }

    #[tokio::test]
    async fn encode_have_zero() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Have { piece_index: 0 }),
            vec![0, 0, 0, 5, 4, 0, 0, 0, 0]
        );
    }

    #[tokio::test]
    async fn encode_have_max_index() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Have {
                piece_index: u32::MAX
            }),
            vec![0, 0, 0, 5, 4, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    #[tokio::test]
    async fn encode_have_256() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Have { piece_index: 256 }),
            vec![0, 0, 0, 5, 4, 0, 0, 1, 0]
        );
    }

    #[tokio::test]
    async fn encode_bitfield_empty() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Bitfield(vec![])),
            vec![0, 0, 0, 1, 5]
        );
    }

    #[tokio::test]
    async fn encode_bitfield_single_byte() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Bitfield(vec![0xFF])),
            vec![0, 0, 0, 2, 5, 0xFF]
        );
    }

    #[tokio::test]
    async fn encode_bitfield_multi_byte() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Bitfield(vec![0xAB, 0xCD])),
            vec![0, 0, 0, 3, 5, 0xAB, 0xCD]
        );
    }

    #[tokio::test]
    async fn encode_request() {
        let conn = make_test_connection().await;
        let mut expected = vec![0, 0, 0, 13, 6];
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&16384u32.to_be_bytes());
        assert_eq!(
            conn.encode_message(&PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 16384
            }),
            expected
        );
    }

    #[tokio::test]
    async fn encode_request_max_values() {
        let conn = make_test_connection().await;
        let mut expected = vec![0, 0, 0, 13, 6];
        expected.extend_from_slice(&u32::MAX.to_be_bytes());
        expected.extend_from_slice(&u32::MAX.to_be_bytes());
        expected.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            conn.encode_message(&PeerMessage::Request {
                index: u32::MAX,
                begin: u32::MAX,
                length: u32::MAX
            }),
            expected
        );
    }

    #[tokio::test]
    async fn encode_piece_empty_data() {
        let conn = make_test_connection().await;
        let mut expected = vec![0, 0, 0, 9, 7];
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(
            conn.encode_message(&PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![]
            }),
            expected
        );
    }

    #[tokio::test]
    async fn encode_piece_with_data() {
        let conn = make_test_connection().await;
        let mut expected = vec![0, 0, 0, 11, 7];
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(&100u32.to_be_bytes());
        expected.extend_from_slice(&[0xDE, 0xAD]);
        assert_eq!(
            conn.encode_message(&PeerMessage::Piece {
                index: 1,
                begin: 100,
                data: vec![0xDE, 0xAD]
            }),
            expected
        );
    }

    #[tokio::test]
    async fn encode_cancel() {
        let conn = make_test_connection().await;
        let mut expected = vec![0, 0, 0, 13, 8];
        expected.extend_from_slice(&5u32.to_be_bytes());
        expected.extend_from_slice(&10u32.to_be_bytes());
        expected.extend_from_slice(&20u32.to_be_bytes());
        assert_eq!(
            conn.encode_message(&PeerMessage::Cancel {
                index: 5,
                begin: 10,
                length: 20
            }),
            expected
        );
    }

    #[tokio::test]
    async fn encode_keep_alive() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::KeepAlive),
            vec![0, 0, 0, 0]
        );
    }

    #[tokio::test]
    async fn encode_port_zero() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Port(0)),
            vec![0, 0, 0, 3, 9, 0, 0]
        );
    }

    #[tokio::test]
    async fn encode_port_6881() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Port(6881)),
            vec![0, 0, 0, 3, 9, 0x1A, 0xE1]
        );
    }

    #[tokio::test]
    async fn encode_port_max() {
        let conn = make_test_connection().await;
        assert_eq!(
            conn.encode_message(&PeerMessage::Port(u16::MAX)),
            vec![0, 0, 0, 3, 9, 0xFF, 0xFF]
        );
    }

    // ===== encode message ID byte =====

    #[tokio::test]
    async fn encode_message_id_bytes() {
        let conn = make_test_connection().await;
        assert_eq!(conn.encode_message(&PeerMessage::Choke)[4], 0);
        assert_eq!(conn.encode_message(&PeerMessage::Unchoke)[4], 1);
        assert_eq!(conn.encode_message(&PeerMessage::Interested)[4], 2);
        assert_eq!(conn.encode_message(&PeerMessage::NotInterested)[4], 3);
        assert_eq!(
            conn.encode_message(&PeerMessage::Have { piece_index: 0 })[4],
            4
        );
        assert_eq!(conn.encode_message(&PeerMessage::Bitfield(vec![]))[4], 5);
        assert_eq!(
            conn.encode_message(&PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 0
            })[4],
            6
        );
        assert_eq!(
            conn.encode_message(&PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![]
            })[4],
            7
        );
        assert_eq!(
            conn.encode_message(&PeerMessage::Cancel {
                index: 0,
                begin: 0,
                length: 0
            })[4],
            8
        );
        assert_eq!(conn.encode_message(&PeerMessage::Port(0))[4], 9);
    }

    // ===== encode message length correctness =====

    #[tokio::test]
    async fn encode_message_length_consistency() {
        let conn = make_test_connection().await;
        let messages: Vec<PeerMessage> = vec![
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::Have { piece_index: 42 },
            PeerMessage::Bitfield(vec![0xFF; 10]),
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 100,
            },
            PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![1, 2, 3],
            },
            PeerMessage::Cancel {
                index: 0,
                begin: 0,
                length: 0,
            },
            PeerMessage::KeepAlive,
            PeerMessage::Port(6881),
        ];
        for msg in &messages {
            let data = conn.encode_message(msg);
            let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
            assert_eq!(len, data.len() - 4, "length mismatch for {:?}", msg);
        }
    }

    // ===== decode_message =====

    #[tokio::test]
    async fn decode_choke() {
        let mut conn = make_test_connection().await;
        assert_eq!(conn.decode_message(&[0]).unwrap(), PeerMessage::Choke);
    }

    #[tokio::test]
    async fn decode_unchoke() {
        let mut conn = make_test_connection().await;
        assert_eq!(conn.decode_message(&[1]).unwrap(), PeerMessage::Unchoke);
    }

    #[tokio::test]
    async fn decode_interested() {
        let mut conn = make_test_connection().await;
        assert_eq!(conn.decode_message(&[2]).unwrap(), PeerMessage::Interested);
    }

    #[tokio::test]
    async fn decode_not_interested() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[3]).unwrap(),
            PeerMessage::NotInterested
        );
    }

    #[tokio::test]
    async fn decode_have_zero() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[4, 0, 0, 0, 0]).unwrap(),
            PeerMessage::Have { piece_index: 0 }
        );
    }

    #[tokio::test]
    async fn decode_have_max() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[4, 0xFF, 0xFF, 0xFF, 0xFF]).unwrap(),
            PeerMessage::Have {
                piece_index: u32::MAX
            }
        );
    }

    #[tokio::test]
    async fn decode_have_256() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[4, 0, 0, 1, 0]).unwrap(),
            PeerMessage::Have { piece_index: 256 }
        );
    }

    #[tokio::test]
    async fn decode_have_too_short() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[4, 0, 0]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("invalid have")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_have_empty_payload() {
        let mut conn = make_test_connection().await;
        assert!(conn.decode_message(&[4]).is_err());
    }

    #[tokio::test]
    async fn decode_bitfield_empty() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[5]).unwrap(),
            PeerMessage::Bitfield(vec![])
        );
    }

    #[tokio::test]
    async fn decode_bitfield_single_byte() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[5, 0xFF]).unwrap(),
            PeerMessage::Bitfield(vec![0xFF])
        );
    }

    #[tokio::test]
    async fn decode_bitfield_multi_byte() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[5, 0xAB, 0xCD, 0xEF]).unwrap(),
            PeerMessage::Bitfield(vec![0xAB, 0xCD, 0xEF])
        );
    }

    #[tokio::test]
    async fn decode_bitfield_updates_peer_bitfield() {
        let mut conn = make_test_connection().await;
        assert!(conn.peer_bitfield.is_empty());
        conn.decode_message(&[5, 0xFF, 0x00]).unwrap();
        assert_eq!(conn.peer_bitfield, vec![0xFF, 0x00]);
    }

    #[tokio::test]
    async fn decode_bitfield_overwrites_previous() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0xFF; 10];
        conn.decode_message(&[5, 0x01]).unwrap();
        assert_eq!(conn.peer_bitfield, vec![0x01]);
    }

    #[tokio::test]
    async fn decode_request() {
        let mut conn = make_test_connection().await;
        let mut payload = vec![6u8];
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&2u32.to_be_bytes());
        payload.extend_from_slice(&16384u32.to_be_bytes());
        assert_eq!(
            conn.decode_message(&payload).unwrap(),
            PeerMessage::Request {
                index: 1,
                begin: 2,
                length: 16384
            }
        );
    }

    #[tokio::test]
    async fn decode_request_too_short() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[6, 0, 0, 0, 1, 0, 0]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("invalid request")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_piece() {
        let mut conn = make_test_connection().await;
        let mut payload = vec![7u8];
        payload.extend_from_slice(&5u32.to_be_bytes());
        payload.extend_from_slice(&100u32.to_be_bytes());
        payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(
            conn.decode_message(&payload).unwrap(),
            PeerMessage::Piece {
                index: 5,
                begin: 100,
                data: vec![0xDE, 0xAD, 0xBE, 0xEF]
            }
        );
    }

    #[tokio::test]
    async fn decode_piece_empty_block() {
        let mut conn = make_test_connection().await;
        let mut payload = vec![7u8];
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(
            conn.decode_message(&payload).unwrap(),
            PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![]
            }
        );
    }

    #[tokio::test]
    async fn decode_piece_too_short() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[7, 0, 0, 0, 1]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("invalid piece")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_cancel() {
        let mut conn = make_test_connection().await;
        let mut payload = vec![8u8];
        payload.extend_from_slice(&3u32.to_be_bytes());
        payload.extend_from_slice(&7u32.to_be_bytes());
        payload.extend_from_slice(&100u32.to_be_bytes());
        assert_eq!(
            conn.decode_message(&payload).unwrap(),
            PeerMessage::Cancel {
                index: 3,
                begin: 7,
                length: 100
            }
        );
    }

    #[tokio::test]
    async fn decode_cancel_too_short() {
        let mut conn = make_test_connection().await;
        let err = conn
            .decode_message(&[8, 0, 0, 0, 1, 0, 0, 0, 2])
            .unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("invalid cancel")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_port_zero() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[9, 0, 0]).unwrap(),
            PeerMessage::Port(0)
        );
    }

    #[tokio::test]
    async fn decode_port_6881() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[9, 0x1A, 0xE1]).unwrap(),
            PeerMessage::Port(6881)
        );
    }

    #[tokio::test]
    async fn decode_port_max() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[9, 0xFF, 0xFF]).unwrap(),
            PeerMessage::Port(u16::MAX)
        );
    }

    #[tokio::test]
    async fn decode_port_too_short() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[9, 0x00]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("invalid port")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_unknown_message_id_255() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[255]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("unknown message id: 255")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_unknown_message_id_10() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[10]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("unknown message id: 10")),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[tokio::test]
    async fn decode_empty_payload() {
        let mut conn = make_test_connection().await;
        let err = conn.decode_message(&[]).unwrap_err();
        match err {
            PeerError::Protocol(msg) => assert!(msg.contains("empty message")),
            _ => panic!("Expected Protocol error"),
        }
    }

    // ===== encode-decode roundtrip =====

    #[tokio::test]
    async fn roundtrip_all_messages() {
        let messages = vec![
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::Have { piece_index: 999 },
            PeerMessage::Bitfield(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            PeerMessage::Request {
                index: 1,
                begin: 2,
                length: 3,
            },
            PeerMessage::Piece {
                index: 4,
                begin: 5,
                data: vec![6, 7, 8],
            },
            PeerMessage::Cancel {
                index: 9,
                begin: 10,
                length: 11,
            },
            PeerMessage::Port(8080),
        ];
        for original in &messages {
            let conn = make_test_connection().await;
            let encoded = conn.encode_message(original);
            let mut conn2 = make_test_connection().await;
            let decoded = conn2.decode_message(&encoded[4..]).unwrap();
            assert_eq!(&decoded, original, "roundtrip failed for {:?}", original);
        }
    }

    // ===== decode with extra trailing data =====

    #[tokio::test]
    async fn decode_have_extra_data_ignored() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[4, 0, 0, 0, 5, 0xFF, 0xFF]).unwrap(),
            PeerMessage::Have { piece_index: 5 }
        );
    }

    #[tokio::test]
    async fn decode_port_extra_data_ignored() {
        let mut conn = make_test_connection().await;
        assert_eq!(
            conn.decode_message(&[9, 0x1A, 0xE1, 0xFF]).unwrap(),
            PeerMessage::Port(6881)
        );
    }

    // ===== update_state =====

    #[tokio::test]
    async fn update_state_choke() {
        let mut conn = make_test_connection().await;
        conn.peer_choking = false;
        conn.update_state(&PeerMessage::Choke);
        assert!(conn.peer_choking);
    }

    #[tokio::test]
    async fn update_state_unchoke() {
        let mut conn = make_test_connection().await;
        assert!(conn.peer_choking);
        conn.update_state(&PeerMessage::Unchoke);
        assert!(!conn.peer_choking);
    }

    #[tokio::test]
    async fn update_state_interested() {
        let mut conn = make_test_connection().await;
        assert!(!conn.peer_interested);
        conn.update_state(&PeerMessage::Interested);
        assert!(conn.peer_interested);
    }

    #[tokio::test]
    async fn update_state_not_interested() {
        let mut conn = make_test_connection().await;
        conn.peer_interested = true;
        conn.update_state(&PeerMessage::NotInterested);
        assert!(!conn.peer_interested);
    }

    #[tokio::test]
    async fn update_state_other_messages_no_effect() {
        let mut conn = make_test_connection().await;
        let initial_choking = conn.peer_choking;
        let initial_interested = conn.peer_interested;
        let others = vec![
            PeerMessage::Have { piece_index: 0 },
            PeerMessage::Bitfield(vec![0xFF]),
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 0,
            },
            PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![],
            },
            PeerMessage::Cancel {
                index: 0,
                begin: 0,
                length: 0,
            },
            PeerMessage::KeepAlive,
            PeerMessage::Port(6881),
        ];
        for msg in &others {
            conn.update_state(msg);
            assert_eq!(
                conn.peer_choking, initial_choking,
                "choking changed by {:?}",
                msg
            );
            assert_eq!(
                conn.peer_interested, initial_interested,
                "interested changed by {:?}",
                msg
            );
        }
    }

    #[tokio::test]
    async fn update_state_sequence_choke_unchoke() {
        let mut conn = make_test_connection().await;
        conn.update_state(&PeerMessage::Choke);
        assert!(conn.peer_choking);
        conn.update_state(&PeerMessage::Unchoke);
        assert!(!conn.peer_choking);
    }

    #[tokio::test]
    async fn update_state_sequence_interested_not_interested() {
        let mut conn = make_test_connection().await;
        conn.update_state(&PeerMessage::Interested);
        assert!(conn.peer_interested);
        conn.update_state(&PeerMessage::NotInterested);
        assert!(!conn.peer_interested);
    }

    #[tokio::test]
    async fn update_state_double_choke_idempotent() {
        let mut conn = make_test_connection().await;
        conn.update_state(&PeerMessage::Choke);
        conn.update_state(&PeerMessage::Choke);
        assert!(conn.peer_choking);
    }

    #[tokio::test]
    async fn update_state_double_unchoke_idempotent() {
        let mut conn = make_test_connection().await;
        conn.update_state(&PeerMessage::Unchoke);
        conn.update_state(&PeerMessage::Unchoke);
        assert!(!conn.peer_choking);
    }

    // ===== has_piece =====

    #[tokio::test]
    async fn has_piece_empty_bitfield() {
        let conn = make_test_connection().await;
        assert!(!conn.has_piece(0));
        assert!(!conn.has_piece(100));
    }

    #[tokio::test]
    async fn has_piece_single_byte_all_set() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0xFF];
        for i in 0..8 {
            assert!(conn.has_piece(i), "should have piece {}", i);
        }
        assert!(!conn.has_piece(8));
    }

    #[tokio::test]
    async fn has_piece_single_byte_none_set() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0x00];
        for i in 0..8 {
            assert!(!conn.has_piece(i));
        }
    }

    #[tokio::test]
    async fn has_piece_msb_first_encoding() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0x80]; // bit 7 = piece 0
        assert!(conn.has_piece(0));
        for i in 1..8 {
            assert!(!conn.has_piece(i));
        }
    }

    #[tokio::test]
    async fn has_piece_second_byte() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0x00, 0x80]; // piece 8
        assert!(!conn.has_piece(0));
        assert!(conn.has_piece(8));
        assert!(!conn.has_piece(9));
    }

    #[tokio::test]
    async fn has_piece_all_bits_second_byte() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0x00, 0xFF];
        for i in 0..8 {
            assert!(!conn.has_piece(i));
        }
        for i in 8..16 {
            assert!(conn.has_piece(i));
        }
        assert!(!conn.has_piece(16));
    }

    #[tokio::test]
    async fn has_piece_out_of_bounds() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0xFF];
        assert!(!conn.has_piece(100));
        assert!(!conn.has_piece(1000));
        assert!(!conn.has_piece(u32::MAX));
    }

    #[tokio::test]
    async fn has_piece_specific_pattern() {
        // 0b10100000 = 0xA0 => pieces 0 and 2
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0xA0];
        assert!(conn.has_piece(0));
        assert!(!conn.has_piece(1));
        assert!(conn.has_piece(2));
        for i in 3..8 {
            assert!(!conn.has_piece(i));
        }
    }

    #[tokio::test]
    async fn has_piece_multi_byte_pattern() {
        let mut conn = make_test_connection().await;
        conn.peer_bitfield = vec![0xFF, 0x00, 0xFF];
        for i in 0..8 {
            assert!(conn.has_piece(i));
        }
        for i in 8..16 {
            assert!(!conn.has_piece(i));
        }
        for i in 16..24 {
            assert!(conn.has_piece(i));
        }
        assert!(!conn.has_piece(24));
    }

    #[tokio::test]
    async fn has_piece_after_bitfield_decode() {
        let mut conn = make_test_connection().await;
        assert!(!conn.has_piece(0));
        conn.decode_message(&[5, 0xFF]).unwrap();
        assert!(conn.has_piece(0));
        assert!(conn.has_piece(7));
        assert!(!conn.has_piece(8));
    }

    // ===== is_choking / is_interested accessors =====

    #[tokio::test]
    async fn is_choking_initial_true() {
        let conn = make_test_connection().await;
        assert!(conn.is_choking());
    }

    #[tokio::test]
    async fn is_choking_after_unchoke() {
        let mut conn = make_test_connection().await;
        conn.update_state(&PeerMessage::Unchoke);
        assert!(!conn.is_choking());
    }

    #[tokio::test]
    async fn is_interested_initial_false() {
        let conn = make_test_connection().await;
        assert!(!conn.is_interested());
    }

    #[tokio::test]
    async fn is_interested_after_interested() {
        let mut conn = make_test_connection().await;
        conn.update_state(&PeerMessage::Interested);
        assert!(conn.is_interested());
    }

    // ===== peer_id accessor =====

    #[tokio::test]
    async fn peer_id_initial_zero() {
        let conn = make_test_connection().await;
        assert_eq!(conn.peer_id(), &[0u8; 20]);
    }

    // ===== Existing async proxy tests =====

    #[tokio::test]
    async fn test_connect_with_http_proxy_rejected() {
        let proxy = ProxyConfig::new(ProxyType::Http, "127.0.0.1".into(), 8080);
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let result = PeerConnection::connect_with_proxy(addr, [0u8; 20], [0u8; 20], &proxy).await;
        assert!(result.is_err());
        if let Err(e) = result {
            match e {
                PeerError::Proxy(msg) => assert!(msg.contains("HTTP CONNECT")),
                _ => panic!("Expected PeerError::Proxy"),
            }
        }
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_timeout() {
        let proxy = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 9999);
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let result = PeerConnection::connect_with_proxy(addr, [0u8; 20], [0u8; 20], &proxy).await;
        assert!(result.is_err());
        if let Err(e) = result {
            match e {
                PeerError::Timeout | PeerError::Proxy(_) => {}
                _ => panic!("Expected Timeout or Proxy error"),
            }
        }
    }

    #[tokio::test]
    async fn test_connect_with_socks5_proxy_with_auth() {
        let proxy = ProxyConfig::with_auth(
            ProxyType::Socks5,
            "127.0.0.1".into(),
            9999,
            "user".into(),
            "pass".into(),
        );
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let result = PeerConnection::connect_with_proxy(addr, [0u8; 20], [0u8; 20], &proxy).await;
        assert!(result.is_err());
        if let Err(e) = result {
            match e {
                PeerError::Timeout | PeerError::Proxy(_) | PeerError::Io(_) => {}
                _ => panic!("Expected connection error"),
            }
        }
    }
}
