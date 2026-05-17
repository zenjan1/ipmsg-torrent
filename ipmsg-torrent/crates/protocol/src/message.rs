use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Base58-encoded PeerID string
pub type PeerIdStr = String;

/// Channel identifier for group/location channels
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChannelId {
    /// Direct message to a specific peer
    Direct(PeerIdStr),
    /// Named group channel
    Group(String),
    /// Geohash-based location channel
    Geohash(String),
}

impl ChannelId {
    pub fn label(&self) -> String {
        match self {
            ChannelId::Direct(pid) => format!("dm:{}", &pid[..8]),
            ChannelId::Group(name) => format!("#{}", name),
            ChannelId::Geohash(hash) => format!("@{}", hash),
        }
    }
}

/// Chat message with sequencing and E2E encryption support
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChatMessage {
    /// Unique message ID (UUID v4)
    pub id: String,
    /// Sender's PeerID (base58)
    pub from: PeerIdStr,
    /// Optional recipient (None for broadcast/channel)
    pub to: Option<PeerIdStr>,
    /// Channel for group/location messages
    pub channel: Option<ChannelId>,
    /// Monotonically increasing sequence number (per sender)
    pub seq: u64,
    /// Message timestamp (UTC)
    pub timestamp: DateTime<Utc>,
    /// Time-to-live in seconds (0 = permanent)
    pub ttl: u64,
    /// Message content and type
    pub kind: MessageType,
    /// Optional encrypted payload (when E2E encryption is used)
    #[serde(default)]
    pub encrypted_payload: Option<EncryptedPayload>,
    /// Ed25519 signature of message content (for auth)
    #[serde(default)]
    pub signature: Vec<u8>,
    /// Optional reply-to message ID
    #[serde(default)]
    pub reply_to: Option<String>,
}

impl ChatMessage {
    pub fn new_text(from: PeerIdStr, to: Option<PeerIdStr>, content: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            from,
            to,
            channel: None,
            seq: 0,
            timestamp: Utc::now(),
            ttl: 0,
            kind: MessageType::Text { content },
            encrypted_payload: None,
            signature: Vec::new(),
            reply_to: None,
        }
    }

    pub fn for_channel(from: PeerIdStr, channel: ChannelId, content: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            from,
            to: None,
            channel: Some(channel),
            seq: 0,
            timestamp: Utc::now(),
            ttl: 0,
            kind: MessageType::Text { content },
            encrypted_payload: None,
            signature: Vec::new(),
            reply_to: None,
        }
    }

    pub fn new_presence(from: PeerIdStr, username: String, platforms: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            from,
            to: None,
            channel: None,
            seq: 0,
            timestamp: Utc::now(),
            ttl: 0,
            kind: MessageType::Presence { username, platforms, bio: None },
            encrypted_payload: None,
            signature: Vec::new(),
            reply_to: None,
        }
    }

    pub fn new_typing(from: PeerIdStr, to: PeerIdStr) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            from,
            to: Some(to),
            channel: None,
            seq: 0,
            timestamp: Utc::now(),
            ttl: 5, // 5 second TTL for typing indicators
            kind: MessageType::Typing,
            encrypted_payload: None,
            signature: Vec::new(),
            reply_to: None,
        }
    }

    pub fn new_command(from: PeerIdStr, command: String, args: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            from,
            to: None,
            channel: None,
            seq: 0,
            timestamp: Utc::now(),
            ttl: 0,
            kind: MessageType::Command { command, args },
            encrypted_payload: None,
            signature: Vec::new(),
            reply_to: None,
        }
    }

    pub fn with_sequence(mut self, seq: u64) -> Self {
        self.seq = seq;
        self
    }

    pub fn with_reply(mut self, reply_to: String) -> Self {
        self.reply_to = Some(reply_to);
        self
    }

    pub fn with_ttl(mut self, ttl_secs: u64) -> Self {
        self.ttl = ttl_secs;
        self
    }

    /// Compute the digest that should be signed
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(self.id.as_bytes());
        hasher.update(self.from.as_bytes());
        let ts_bytes = self.timestamp.timestamp_millis().to_be_bytes();
        hasher.update(ts_bytes);
        let seq_bytes = self.seq.to_be_bytes();
        hasher.update(seq_bytes);
        // Serialize kind to CBOR for signing
        if let Ok(cbor) = serde_cbor::to_vec(&self.kind) {
            hasher.update(&cbor);
        }
        hasher.finalize().to_vec()
    }

    /// Extract text content if this is a Text message
    pub fn text_content(&self) -> Option<&str> {
        match &self.kind {
            MessageType::Text { content } => Some(content),
            _ => None,
        }
    }

    /// Check if this message has expired
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        let age_secs = Utc::now().signed_duration_since(self.timestamp).num_seconds() as u64;
        age_secs > self.ttl
    }
}

/// Encrypted payload for E2E double-ratchet encryption
/// (inspired by SimpleX double-ratchet + BitChat Noise protocol)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EncryptedPayload {
    /// X25519 ephemeral public key (32 bytes, base58)
    pub ephemeral_key: String,
    /// Encrypted ciphertext (includes MAC)
    pub ciphertext: Vec<u8>,
    /// Nonce/IV for the symmetric cipher
    pub nonce: Vec<u8>,
    /// Double-ratchet chain index (for ratchet state synchronization)
    pub ratchet_index: u64,
}

/// Discriminated union of all message types
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MessageType {
    /// Plain text message
    Text { content: String },
    /// Inline image (base64 encoded for small images)
    Image {
        data: Vec<u8>,
        mime_type: String,
        name: String,
    },
    /// File transfer reference (torrent-style chunked)
    File { file_ref: FileRef },
    /// Typing indicator (ephemeral, not stored)
    Typing,
    /// Read receipt
    ReadReceipt { message_id: String },
    /// Presence announcement (broadcast on join / periodic)
    Presence {
        username: String,
        platforms: Vec<String>,
        /// Optional user bio/status message
        bio: Option<String>,
    },
    /// IRC-style command (for CLI interoperability)
    Command {
        command: String,
        args: Vec<String>,
    },
    /// Message acknowledgment (delivery confirmed)
    Ack {
        message_ids: Vec<String>,
    },
    /// Peer profile update
    Profile {
        username: String,
        bio: Option<String>,
        avatar_hash: Option<String>,
    },
}

impl MessageType {
    pub fn label(&self) -> &'static str {
        match self {
            MessageType::Text { .. } => "text",
            MessageType::Image { .. } => "image",
            MessageType::File { .. } => "file",
            MessageType::Typing => "typing",
            MessageType::ReadReceipt { .. } => "read_receipt",
            MessageType::Presence { .. } => "presence",
            MessageType::Command { .. } => "command",
            MessageType::Ack { .. } => "ack",
            MessageType::Profile { .. } => "profile",
        }
    }
}

impl Default for MessageType {
    fn default() -> Self {
        MessageType::Text { content: String::new() }
    }
}

/// Reference to a file being transferred via torrent-style chunked protocol
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileRef {
    /// SHA-256 hash of the complete file (hex string)
    pub hash: String,
    /// Original file name
    pub name: String,
    /// File size in bytes
    pub size: u64,
    /// MIME type
    pub mime_type: String,
    /// Total number of chunks
    pub chunks: u32,
    /// Chunk size in bytes (default 256KB)
    pub chunk_size: u32,
    /// Optional thumbnail data for images
    #[serde(default)]
    pub thumbnail: Option<Vec<u8>>,
}

impl FileRef {
    pub fn new(name: String, size: u64, mime_type: String, data: &[u8]) -> Self {
        let hash = format!("{:x}", Sha256::digest(data));
        let chunk_size: u32 = 256 * 1024; // 256KB
        let chunks = ((size as f64) / (chunk_size as f64)).ceil() as u32;
        Self {
            hash,
            name,
            size,
            mime_type,
            chunks,
            chunk_size,
            thumbnail: None,
        }
    }
}

/// File transfer protocol messages (sent via request-response)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FileTransferMsg {
    /// Offer a file for transfer
    Offer { file_ref: FileRef },
    /// Accept file transfer, ready to receive
    Accept { file_ref: FileRef },
    /// Reject file transfer
    Reject { reason: String },
    /// Send a file chunk
    Chunk {
        file_hash: String,
        index: u32,
        data: Vec<u8>,
    },
    /// Chunk acknowledgment
    ChunkAck { file_hash: String, index: u32 },
    /// Transfer complete
    Complete { file_hash: String },
    /// Transfer error
    Error { file_hash: String, reason: String },
    /// Request re-send of specific missing chunks
    Resend {
        file_hash: String,
        indices: Vec<u32>,
    },
}

/// Geohash utility functions (inspired by BitChat location channels)
pub mod geohash {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

    /// Encode lat/lon to geohash string (precision = chars)
    pub fn encode(lat: f64, lon: f64, precision: usize) -> String {
        let mut lat_range = (-90.0, 90.0);
        let mut lon_range = (-180.0, 180.0);
        let mut bits = 0;
        let mut ch = 0;
        let mut result = String::with_capacity(precision);
        let mut is_lon = true;

        while result.len() < precision {
            let (mid, bit) = if is_lon {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if lon >= mid {
                    lon_range.0 = mid;
                    (mid, 1u8)
                } else {
                    lon_range.1 = mid;
                    (mid, 0u8)
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if lat >= mid {
                    lat_range.0 = mid;
                    (mid, 1u8)
                } else {
                    lat_range.1 = mid;
                    (mid, 0u8)
                }
            };

            ch = (ch << 1) | bit;
            bits += 1;
            is_lon = !is_lon;
            let _ = mid;

            if bits == 5 {
                result.push(BASE32[ch as usize] as char);
                bits = 0;
                ch = 0;
            }
        }
        result
    }

    /// Get parent geohash (one level up)
    pub fn parent(hash: &str) -> Option<String> {
        if hash.is_empty() { None } else { Some(hash[..hash.len() - 1].to_string()) }
    }

    /// Channel name for a given geohash precision level
    pub fn channel_for(lat: f64, lon: f64, precision: usize) -> String {
        encode(lat, lon, precision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = ChatMessage::new_text(
            "peer1".to_string(),
            Some("peer2".to_string()),
            "hello".to_string(),
        );
        assert_eq!(msg.kind.label(), "text");
        assert!(!msg.is_expired());
    }

    #[test]
    fn test_ttl_expiry() {
        let msg = ChatMessage::new_text(
            "peer1".to_string(),
            Some("peer2".to_string()),
            "ephemeral".to_string(),
        )
        .with_ttl(1); // 1 second TTL
        // Should not be expired immediately
        assert!(!msg.is_expired());
    }

    #[test]
    fn test_geohash_encode() {
        let hash = geohash::encode(51.5074, -0.1278, 6);
        assert_eq!(hash.len(), 6);
        assert!(hash.chars().all(|c| BASE32.contains(&(c as u8))));
    }

    #[test]
    fn test_channel_id_label() {
        let direct = ChannelId::Direct("123456789abcdef".to_string());
        assert_eq!(direct.label(), "dm:12345678");

        let group = ChannelId::Group("devs".to_string());
        assert_eq!(group.label(), "#devs");

        let geo = ChannelId::Geohash("u4pruy".to_string());
        assert_eq!(geo.label(), "@u4pruy");
    }
}
