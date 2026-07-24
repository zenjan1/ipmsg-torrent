//! Classic IPMSG (FeiQ/Feige) protocol compatibility layer
//!
//! This module implements the original IPMSG/FeiQ UDP broadcast protocol
//! so this client can interoperate with legacy FeiQ/Feige desktop clients.
//!
//! Protocol format:
//!   VERSION:PACKET_NO:SENDER_NAME:SENDER_HOST:CMD_NO:EXTRA_INFO
//!
//! CMD_NO values:
//!   0x00000001 - IPMSG_SENDMSG (send message)
//!   0x00000003 - IPMSG_ANSLIST (reply to broadcast)
//!   0x00000004 - IPMSG_BR_ENTRY (broadcast entry/announce)
//!   0x00000005 - IPMSG_ANSENTRY (answer to entry)
//!   0x00000006 - IPMSG_BR_EXIT (broadcast exit)
//!   0x00000010 - IPMSG_GETFILEDATA (file transfer request)
//!   0x00000011 - IPMSG_RELEASEFILES (release files)
//!   0x00000012 - IPMSG_GETDIRFILES (get directory listing)
//!   0x00000018 - IPMSG_GETINFO (get peer info)
//!   0x00000020 - IPMSG_SENDINFO (send info response)
//!   0x00000030 - IPMSG_ABSENCE (absence notification)
//!
//! Extended flags (in CMD_NO high bits):
//!   IPMSG_FILEATTACHOPT = 0x00400000
//!   IPMSG_READCHECKOPT  = 0x00200000
//!   IPMSG_SENDCHECKOPT  = 0x00100000
//!   IPMSG_NOADDLISTOPT  = 0x00800000
//!   IPMSG_NOLOGOPT      = 0x01000000
//!   IPMSG_AUTORETOPT    = 0x80000000

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

/// Default IPMSG port (FeiQ uses 2425)
pub const IPMSG_PORT: u16 = 2425;

/// IPMSG command codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum IpMsgCmd {
    SendMsg = 0x00000001,
    AnsList = 0x00000003,
    BrEntry = 0x00000004,
    AnsEntry = 0x00000005,
    BrExit = 0x00000006,
    GetFileData = 0x00000010,
    ReleaseFiles = 0x00000011,
    GetDirFiles = 0x00000012,
    GetInfo = 0x00000018,
    SendInfo = 0x00000020,
    Absence = 0x00000030,
    Unknown(u32),
}

impl From<u32> for IpMsgCmd {
    fn from(v: u32) -> Self {
        match v & 0x000000FF {
            0x01 => IpMsgCmd::SendMsg,
            0x03 => IpMsgCmd::AnsList,
            0x04 => IpMsgCmd::BrEntry,
            0x05 => IpMsgCmd::AnsEntry,
            0x06 => IpMsgCmd::BrExit,
            0x10 => IpMsgCmd::GetFileData,
            0x11 => IpMsgCmd::ReleaseFiles,
            0x12 => IpMsgCmd::GetDirFiles,
            0x18 => IpMsgCmd::GetInfo,
            0x20 => IpMsgCmd::SendInfo,
            0x30 => IpMsgCmd::Absence,
            other => IpMsgCmd::Unknown(other),
        }
    }
}

impl IpMsgCmd {
    pub fn as_u32(&self) -> u32 {
        match self {
            IpMsgCmd::SendMsg => 0x00000001,
            IpMsgCmd::AnsList => 0x00000003,
            IpMsgCmd::BrEntry => 0x00000004,
            IpMsgCmd::AnsEntry => 0x00000005,
            IpMsgCmd::BrExit => 0x00000006,
            IpMsgCmd::GetFileData => 0x00000010,
            IpMsgCmd::ReleaseFiles => 0x00000011,
            IpMsgCmd::GetDirFiles => 0x00000012,
            IpMsgCmd::GetInfo => 0x00000018,
            IpMsgCmd::SendInfo => 0x00000020,
            IpMsgCmd::Absence => 0x00000030,
            IpMsgCmd::Unknown(v) => *v,
        }
    }
}

/// Extended flags for IPMSG commands
pub const IPMSG_FILEATTACHOPT: u32 = 0x00400000;
pub const IPMSG_READCHECKOPT: u32 = 0x00200000;
pub const IPMSG_SENDCHECKOPT: u32 = 0x00100000;
pub const IPMSG_NOADDLISTOPT: u32 = 0x00800000;
pub const IPMSG_NOLOGOPT: u32 = 0x01000000;
pub const IPMSG_AUTORETOPT: u32 = 0x80000000;
pub const IPMSG_FLAGOPT: u32 = 0x00000100;

/// IPMSG protocol version
pub const IPMSG_VERSION: u32 = 1;

/// A parsed IPMSG packet
#[derive(Debug, Clone)]
pub struct IpMsgPacket {
    pub version: u32,
    pub packet_no: u32,
    pub sender_name: String,
    pub sender_host: String,
    pub cmd_no: u32,
    pub extra_info: String,
    pub source_addr: SocketAddr,
}

impl IpMsgPacket {
    /// Parse a raw IPMSG packet
    pub fn parse(data: &[u8], source: SocketAddr) -> Option<Self> {
        let s = String::from_utf8_lossy(data);
        let parts: Vec<&str> = s.splitn(6, ':').collect();
        if parts.len() < 5 {
            return None;
        }

        let version = parts[0].parse::<u32>().ok()?;
        let packet_no = parts[1].parse::<u32>().unwrap_or(0);
        let sender_name = parts[2].to_string();
        let sender_host = parts[3].to_string();
        let cmd_no = parts[4].parse::<u32>().ok()?;
        let extra_info = if parts.len() > 5 {
            parts[5].to_string()
        } else {
            String::new()
        };

        Some(Self {
            version,
            packet_no,
            sender_name,
            sender_host,
            cmd_no,
            extra_info,
            source_addr: source,
        })
    }

    /// Serialize to IPMSG wire format
    pub fn serialize(&self) -> Vec<u8> {
        format!(
            "{}:{}:{}:{}:{}:{}",
            self.version,
            self.packet_no,
            self.sender_name,
            self.sender_host,
            self.cmd_no,
            self.extra_info
        )
        .into_bytes()
    }

    /// Get the command enum
    pub fn cmd(&self) -> IpMsgCmd {
        IpMsgCmd::from(self.cmd_no)
    }

    /// Check if a flag is set in cmd_no
    pub fn has_flag(&self, flag: u32) -> bool {
        (self.cmd_no & flag) != 0
    }

    /// Extract message content from SendMsg packet
    pub fn message_content(&self) -> Option<String> {
        if self.cmd() != IpMsgCmd::SendMsg {
            return None;
        }
        // Extra info format for SendMsg: message_body[\0attach_info]
        let content = self.extra_info.split('\0').next().unwrap_or("");
        if content.is_empty() {
            None
        } else {
            Some(content.to_string())
        }
    }

    /// Extract attachment info (file list) from SendMsg packet
    pub fn attachment_info(&self) -> Option<String> {
        if !self.has_flag(IPMSG_FILEATTACHOPT) {
            return None;
        }
        let parts: Vec<&str> = self.extra_info.splitn(2, '\0').collect();
        if parts.len() > 1 {
            Some(parts[1].to_string())
        } else {
            None
        }
    }
}

/// Builder for creating IPMSG packets
pub struct IpMsgPacketBuilder {
    packet_no: u32,
    sender_name: String,
    sender_host: String,
}

impl IpMsgPacketBuilder {
    pub fn new(sender_name: String, sender_host: String) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(1);
        Self {
            packet_no: COUNTER.fetch_add(1, Ordering::Relaxed),
            sender_name,
            sender_host,
        }
    }

    /// Build a BrEntry (broadcast entry) packet
    pub fn br_entry(&self) -> IpMsgPacket {
        IpMsgPacket {
            version: IPMSG_VERSION,
            packet_no: self.packet_no,
            sender_name: self.sender_name.clone(),
            sender_host: self.sender_host.clone(),
            cmd_no: IpMsgCmd::BrEntry.as_u32(),
            extra_info: self.sender_name.clone(),
            source_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }

    /// Build an AnsEntry (answer to entry) packet
    pub fn ans_entry(&self) -> IpMsgPacket {
        IpMsgPacket {
            version: IPMSG_VERSION,
            packet_no: self.packet_no,
            sender_name: self.sender_name.clone(),
            sender_host: self.sender_host.clone(),
            cmd_no: IpMsgCmd::AnsEntry.as_u32(),
            extra_info: self.sender_name.clone(),
            source_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }

    /// Build a SendMsg packet
    pub fn send_msg(&self, message: &str) -> IpMsgPacket {
        IpMsgPacket {
            version: IPMSG_VERSION,
            packet_no: self.packet_no,
            sender_name: self.sender_name.clone(),
            sender_host: self.sender_host.clone(),
            cmd_no: IpMsgCmd::SendMsg.as_u32() | IPMSG_SENDCHECKOPT,
            extra_info: message.to_string(),
            source_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }

    /// Build a SendMsg with file attachment
    pub fn send_msg_with_file(&self, message: &str, file_info: &str) -> IpMsgPacket {
        IpMsgPacket {
            version: IPMSG_VERSION,
            packet_no: self.packet_no,
            sender_name: self.sender_name.clone(),
            sender_host: self.sender_host.clone(),
            cmd_no: IpMsgCmd::SendMsg.as_u32() | IPMSG_FILEATTACHOPT | IPMSG_SENDCHECKOPT,
            extra_info: format!("{}\0{}", message, file_info),
            source_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }

    /// Build a BrExit packet
    pub fn br_exit(&self) -> IpMsgPacket {
        IpMsgPacket {
            version: IPMSG_VERSION,
            packet_no: self.packet_no,
            sender_name: self.sender_name.clone(),
            sender_host: self.sender_host.clone(),
            cmd_no: IpMsgCmd::BrExit.as_u32(),
            extra_info: String::new(),
            source_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }

    /// Build a SendInfo packet (response to GetInfo)
    pub fn send_info(&self, info: &str) -> IpMsgPacket {
        IpMsgPacket {
            version: IPMSG_VERSION,
            packet_no: self.packet_no,
            sender_name: self.sender_name.clone(),
            sender_host: self.sender_host.clone(),
            cmd_no: IpMsgCmd::SendInfo.as_u32(),
            extra_info: info.to_string(),
            source_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }
}

/// IPMSG file attachment info (for file list in extra_info)
/// Format: file_id:filename:size:mtime:file_type
#[derive(Debug, Clone)]
pub struct IpMsgFileEntry {
    pub file_id: u32,
    pub filename: String,
    pub size: u64,
    pub mtime: u64,
    pub file_type: String,
}

impl IpMsgFileEntry {
    pub fn serialize(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.file_id, self.filename, self.size, self.mtime, self.file_type
        )
    }

    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.splitn(5, ':').collect();
        if parts.len() < 4 {
            return None;
        }
        Some(Self {
            file_id: parts[0].parse().ok()?,
            filename: parts[1].to_string(),
            size: parts[2].parse().unwrap_or(0),
            mtime: parts[3].parse().unwrap_or(0),
            file_type: if parts.len() > 4 {
                parts[4].to_string()
            } else {
                String::new()
            },
        })
    }
}

/// Classic IPMSG UDP listener/sender
pub struct IpMsgCompat {
    socket: Option<Arc<UdpSocket>>,
    builder: IpMsgPacketBuilder,
    known_peers: Arc<Mutex<HashMap<String, IpMsgPeerInfo>>>,
    username: String,
    hostname: String,
}

/// Info about a discovered classic IPMSG peer
#[derive(Debug, Clone)]
pub struct IpMsgPeerInfo {
    pub name: String,
    pub host: String,
    pub addr: SocketAddr,
    pub last_seen: std::time::Instant,
}

impl IpMsgCompat {
    pub fn new(username: String) -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let builder = IpMsgPacketBuilder::new(username.clone(), hostname.clone());
        Self {
            socket: None,
            builder,
            known_peers: Arc::new(Mutex::new(HashMap::new())),
            username,
            hostname,
        }
    }

    /// Start listening on the IPMSG port
    pub async fn start(&mut self) -> Result<(), std::io::Error> {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", IPMSG_PORT)).await?;
        socket.set_broadcast(true)?;
        self.socket = Some(Arc::new(socket));

        // Broadcast our entry
        self.broadcast_entry().await?;

        Ok(())
    }

    /// Broadcast BrEntry to announce presence
    pub async fn broadcast_entry(&self) -> Result<(), std::io::Error> {
        if let Some(socket) = &self.socket {
            let packet = self.builder.br_entry();
            let data = packet.serialize();
            let broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), IPMSG_PORT);
            socket.send_to(&data, broadcast).await?;
            tracing::info!("Broadcast IPMSG BrEntry");
        }
        Ok(())
    }

    /// Send AnsEntry reply to a peer
    pub async fn reply_entry(&self, to: SocketAddr) -> Result<(), std::io::Error> {
        if let Some(socket) = &self.socket {
            let packet = self.builder.ans_entry();
            let data = packet.serialize();
            socket.send_to(&data, to).await?;
        }
        Ok(())
    }

    /// Send a text message to a classic IPMSG peer
    pub async fn send_message(&self, to: SocketAddr, message: &str) -> Result<(), std::io::Error> {
        if let Some(socket) = &self.socket {
            let packet = self.builder.send_msg(message);
            let data = packet.serialize();
            socket.send_to(&data, to).await?;
            tracing::info!(addr = %to, "Sent IPMSG message");
        }
        Ok(())
    }

    /// Broadcast exit announcement
    pub async fn broadcast_exit(&self) -> Result<(), std::io::Error> {
        if let Some(socket) = &self.socket {
            let packet = self.builder.br_exit();
            let data = packet.serialize();
            let broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), IPMSG_PORT);
            socket.send_to(&data, broadcast).await?;
        }
        Ok(())
    }

    /// Poll for incoming packets (non-blocking)
    pub async fn poll_packet(&self) -> Option<IpMsgPacket> {
        let socket = self.socket.as_ref()?;
        let mut buf = vec![0u8; 65536];
        match socket.try_recv_from(&mut buf) {
            Ok((len, addr)) => IpMsgPacket::parse(&buf[..len], addr),
            Err(_) => None,
        }
    }

    /// Receive an incoming packet (blocking - awaits UDP data)
    pub async fn recv_packet(&self) -> Option<IpMsgPacket> {
        let socket = self.socket.as_ref()?;
        let mut buf = vec![0u8; 65536];
        match socket.recv_from(&mut buf).await {
            Ok((len, addr)) => IpMsgPacket::parse(&buf[..len], addr),
            Err(e) => {
                tracing::warn!(error = %e, "IPMSG UDP recv error");
                None
            }
        }
    }

    /// Get the underlying socket for select purposes
    pub fn socket(&self) -> Option<&UdpSocket> {
        self.socket.as_ref().map(|s| s.as_ref())
    }

    /// Process an incoming packet and update peer state
    pub async fn process_packet(&self, packet: &IpMsgPacket) -> Option<IpMsgCompatEvent> {
        let peer_key = format!("{}@{}", packet.sender_name, packet.source_addr.ip());

        match packet.cmd() {
            IpMsgCmd::BrEntry => {
                // New peer announced - reply with AnsEntry
                let mut peers = self.known_peers.lock().await;
                peers.insert(
                    peer_key.clone(),
                    IpMsgPeerInfo {
                        name: packet.sender_name.clone(),
                        host: packet.sender_host.clone(),
                        addr: packet.source_addr,
                        last_seen: std::time::Instant::now(),
                    },
                );
                drop(peers);
                let _ = self.reply_entry(packet.source_addr).await;
                Some(IpMsgCompatEvent::PeerDiscovered {
                    name: packet.sender_name.clone(),
                    host: packet.sender_host.clone(),
                    addr: packet.source_addr,
                })
            }
            IpMsgCmd::AnsEntry => {
                let mut peers = self.known_peers.lock().await;
                peers.insert(
                    peer_key,
                    IpMsgPeerInfo {
                        name: packet.sender_name.clone(),
                        host: packet.sender_host.clone(),
                        addr: packet.source_addr,
                        last_seen: std::time::Instant::now(),
                    },
                );
                Some(IpMsgCompatEvent::PeerDiscovered {
                    name: packet.sender_name.clone(),
                    host: packet.sender_host.clone(),
                    addr: packet.source_addr,
                })
            }
            IpMsgCmd::SendMsg => {
                if let Some(content) = packet.message_content() {
                    Some(IpMsgCompatEvent::MessageReceived {
                        from: packet.sender_name.clone(),
                        addr: packet.source_addr,
                        content,
                        has_attachment: packet.has_flag(IPMSG_FILEATTACHOPT),
                    })
                } else {
                    None
                }
            }
            IpMsgCmd::BrExit => {
                let mut peers = self.known_peers.lock().await;
                peers.remove(&peer_key);
                Some(IpMsgCompatEvent::PeerLeft {
                    name: packet.sender_name.clone(),
                    addr: packet.source_addr,
                })
            }
            IpMsgCmd::GetInfo => {
                // Respond with our info
                let info = format!(
                    "{}:{}:{}",
                    self.username, self.hostname, "ipmsg-torrent/2.0"
                );
                if let Some(socket) = &self.socket {
                    let packet = self.builder.send_info(&info);
                    let _ = socket
                        .send_to(&packet.serialize(), packet.source_addr)
                        .await;
                }
                None
            }
            _ => None,
        }
    }

    /// Get list of known classic IPMSG peers
    pub async fn known_peers(&self) -> Vec<IpMsgPeerInfo> {
        self.known_peers.lock().await.values().cloned().collect()
    }

    /// Clean up stale peers (not seen for > 5 minutes)
    pub async fn cleanup_stale_peers(&self) {
        let mut peers = self.known_peers.lock().await;
        peers.retain(|_, info| info.last_seen.elapsed() < Duration::from_secs(300));
    }
}

/// Events from the classic IPMSG compatibility layer
#[derive(Debug, Clone)]
pub enum IpMsgCompatEvent {
    PeerDiscovered {
        name: String,
        host: String,
        addr: SocketAddr,
    },
    PeerLeft {
        name: String,
        addr: SocketAddr,
    },
    MessageReceived {
        from: String,
        addr: SocketAddr,
        content: String,
        has_attachment: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_parse() {
        let raw = b"1:100:testuser:TESTHOST:4:testuser";
        let packet =
            IpMsgPacket::parse(raw, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2425))
                .unwrap();
        assert_eq!(packet.version, 1);
        assert_eq!(packet.packet_no, 100);
        assert_eq!(packet.sender_name, "testuser");
        assert_eq!(packet.sender_host, "TESTHOST");
        assert_eq!(packet.cmd(), IpMsgCmd::BrEntry);
    }

    #[test]
    fn test_packet_serialize_roundtrip() {
        let builder = IpMsgPacketBuilder::new("alice".to_string(), "ALICE-PC".to_string());
        let packet = builder.send_msg("Hello World");
        let data = packet.serialize();
        let parsed = IpMsgPacket::parse(
            &data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2425),
        )
        .unwrap();
        assert_eq!(parsed.cmd(), IpMsgCmd::SendMsg);
        assert_eq!(parsed.message_content(), Some("Hello World".to_string()));
    }

    #[test]
    fn test_file_entry() {
        let entry = IpMsgFileEntry {
            file_id: 1,
            filename: "test.txt".to_string(),
            size: 1024,
            mtime: 1700000000,
            file_type: String::new(),
        };
        let s = entry.serialize();
        let parsed = IpMsgFileEntry::parse(&s).unwrap();
        assert_eq!(parsed.filename, "test.txt");
        assert_eq!(parsed.size, 1024);
    }

    #[test]
    fn test_send_msg_with_attachment() {
        let builder = IpMsgPacketBuilder::new("bob".to_string(), "BOB-PC".to_string());
        let file_info = "1:report.pdf:2048:1700000000:";
        let packet = builder.send_msg_with_file("Check this file", file_info);
        assert!(packet.has_flag(IPMSG_FILEATTACHOPT));
        assert_eq!(
            packet.message_content(),
            Some("Check this file".to_string())
        );
        assert_eq!(packet.attachment_info(), Some(file_info.to_string()));
    }
}
