//! eDonkey protocol types and constants

use std::net::SocketAddr;

/// eDonkey chunk size: 9.28 MB (9728000 bytes)
pub const ED2K_CHUNK_SIZE: u64 = 9_728_000;

/// eDonkey block size: 180 KB (184320 bytes)
pub const ED2K_BLOCK_SIZE: u64 = 184_320;

/// eDonkey file hash (MD4, 16 bytes)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ed2kFileHash(pub [u8; 16]);

impl Ed2kFileHash {
    pub fn from_hex(hex: &str) -> Result<Self, hex::FromHexError> {
        let bytes = hex::decode(hex)?;
        if bytes.len() != 16 {
            return Err(hex::FromHexError::InvalidStringLength);
        }
        let mut hash = [0u8; 16];
        hash.copy_from_slice(&bytes);
        Ok(Self(hash))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

/// eDonkey peer information
#[derive(Debug, Clone)]
pub struct Ed2kPeer {
    pub addr: SocketAddr,
    pub peer_id: [u8; 16],
    pub server_ip: Option<std::net::Ipv4Addr>,
    pub server_port: Option<u16>,
    pub client_software: String,
}

/// eDonkey message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Ed2kClientOpcode {
    LoginRequest = 0x01,
    GetServerList = 0x14,
    OfferFiles = 0x15,
    SearchRequest = 0x16,
    GetSources = 0x19,
    CallbackRequest = 0x1C,
    QueryMoreResults = 0x21,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Ed2kServerOpcode {
    LoginAnswer = 0x20,
    ServerMessage = 0x38,
    ServerList = 0x32,
    SearchResult = 0x33,
    ServerStatus = 0x34,
    CallbackRequested = 0x35,
    CallbackFailed = 0x36,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Ed2kPeerOpcode {
    Hello = 0x01,
    GetSources = 0x19,
    FileAnswer = 0x48,
    HashSet = 0x51,
    StartUploadReq = 0x52,
    AcceptUploadReq = 0x53,
    QueueRank = 0x5C,
    FileNotFound = 0x49,
}

/// eDonkey file status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ed2kFileStatus {
    Unknown,
    Hashing,
    Complete,
    Downloading,
    Paused,
    Queued,
}

/// eDonkey search result
#[derive(Debug, Clone)]
pub struct Ed2kSearchResult {
    pub name: String,
    pub size: u64,
    pub hash: Ed2kFileHash,
    pub sources: u32,
    pub complete_sources: u32,
    pub media_length: Option<u32>,
    pub media_bitrate: Option<u32>,
    pub media_codec: Option<String>,
}
