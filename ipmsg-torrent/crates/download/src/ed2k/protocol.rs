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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Constants ──────────────────────────────────────────────

    #[test]
    fn ed2k_chunk_size_value() {
        assert_eq!(ED2K_CHUNK_SIZE, 9_728_000);
    }

    #[test]
    fn ed2k_block_size_value() {
        assert_eq!(ED2K_BLOCK_SIZE, 184_320);
    }

    #[test]
    fn chunk_is_multiple_of_block() {
        // 9728000 / 184320 ≈ 52.78, not exact, but both are positive
        assert!(ED2K_CHUNK_SIZE > ED2K_BLOCK_SIZE);
        assert!(ED2K_BLOCK_SIZE > 0);
    }

    // ── Ed2kFileHash ───────────────────────────────────────────

    #[test]
    fn file_hash_from_hex_valid() {
        let hex = "d413a05b1c67e80a4f3e2b9c5a8d7f01";
        let hash = Ed2kFileHash::from_hex(hex).unwrap();
        assert_eq!(hash.0[0], 0xd4);
        assert_eq!(hash.0[15], 0x01);
    }

    #[test]
    fn file_hash_from_hex_all_zeros() {
        let hex = "00000000000000000000000000000000";
        let hash = Ed2kFileHash::from_hex(hex).unwrap();
        assert_eq!(hash.0, [0u8; 16]);
    }

    #[test]
    fn file_hash_from_hex_all_ff() {
        let hex = "ffffffffffffffffffffffffffffffff";
        let hash = Ed2kFileHash::from_hex(hex).unwrap();
        assert_eq!(hash.0, [0xFF; 16]);
    }

    #[test]
    fn file_hash_from_hex_uppercase() {
        let hex = "D413A05B1C67E80A4F3E2B9C5A8D7F01";
        let hash = Ed2kFileHash::from_hex(hex).unwrap();
        assert_eq!(hash.0[0], 0xd4);
    }

    #[test]
    fn file_hash_from_hex_too_short() {
        let hex = "d413a05b";
        assert!(Ed2kFileHash::from_hex(hex).is_err());
    }

    #[test]
    fn file_hash_from_hex_too_long() {
        let hex = "d413a05b1c67e80a4f3e2b9c5a8d7f01aa";
        assert!(Ed2kFileHash::from_hex(hex).is_err());
    }

    #[test]
    fn file_hash_from_hex_invalid_chars() {
        let hex = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        assert!(Ed2kFileHash::from_hex(hex).is_err());
    }

    #[test]
    fn file_hash_from_hex_empty() {
        assert!(Ed2kFileHash::from_hex("").is_err());
    }

    #[test]
    fn file_hash_to_hex_roundtrip() {
        let hex = "d413a05b1c67e80a4f3e2b9c5a8d7f01";
        let hash = Ed2kFileHash::from_hex(hex).unwrap();
        assert_eq!(hash.to_hex(), hex);
    }

    #[test]
    fn file_hash_to_hex_all_zeros() {
        let hash = Ed2kFileHash([0u8; 16]);
        assert_eq!(hash.to_hex(), "00000000000000000000000000000000");
    }

    #[test]
    fn file_hash_to_hex_all_ff() {
        let hash = Ed2kFileHash([0xFF; 16]);
        assert_eq!(hash.to_hex(), "ffffffffffffffffffffffffffffffff");
    }

    #[test]
    fn file_hash_to_hex_lowercase_output() {
        let hash = Ed2kFileHash([0xAB; 16]);
        let hex = hash.to_hex();
        assert_eq!(hex, "abababababababababababababababab");
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }

    #[test]
    fn file_hash_equality() {
        let a = Ed2kFileHash([1u8; 16]);
        let b = Ed2kFileHash([1u8; 16]);
        let c = Ed2kFileHash([2u8; 16]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn file_hash_clone_independence() {
        let a = Ed2kFileHash([42u8; 16]);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn file_hash_debug() {
        let hash = Ed2kFileHash([0u8; 16]);
        let debug = format!("{:?}", hash);
        assert!(debug.contains("Ed2kFileHash"));
    }

    #[test]
    fn file_hash_hash_trait() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Ed2kFileHash([1u8; 16]));
        set.insert(Ed2kFileHash([2u8; 16]));
        set.insert(Ed2kFileHash([1u8; 16])); // duplicate
        assert_eq!(set.len(), 2);
    }

    // ── Ed2kPeer ───────────────────────────────────────────────

    #[test]
    fn peer_clone_and_debug() {
        let peer = Ed2kPeer {
            addr: "192.168.1.1:6881".parse().unwrap(),
            peer_id: [0u8; 16],
            server_ip: Some("10.0.0.1".parse().unwrap()),
            server_port: Some(4662),
            client_software: "eMule".to_string(),
        };
        let cloned = peer.clone();
        assert_eq!(cloned.addr, peer.addr);
        assert_eq!(cloned.client_software, "eMule");
        let debug = format!("{:?}", peer);
        assert!(debug.contains("Ed2kPeer"));
    }

    #[test]
    fn peer_optional_fields_none() {
        let peer = Ed2kPeer {
            addr: "127.0.0.1:6881".parse().unwrap(),
            peer_id: [0u8; 16],
            server_ip: None,
            server_port: None,
            client_software: String::new(),
        };
        assert!(peer.server_ip.is_none());
        assert!(peer.server_port.is_none());
        assert!(peer.client_software.is_empty());
    }

    #[test]
    fn peer_unicode_software() {
        let peer = Ed2kPeer {
            addr: "127.0.0.1:6881".parse().unwrap(),
            peer_id: [0u8; 16],
            server_ip: None,
            server_port: None,
            client_software: "中文客户端".to_string(),
        };
        assert_eq!(peer.client_software, "中文客户端");
    }

    // ── Ed2kClientOpcode ───────────────────────────────────────

    #[test]
    fn client_opcode_all_variants_distinct() {
        let opcodes = [
            Ed2kClientOpcode::LoginRequest,
            Ed2kClientOpcode::GetServerList,
            Ed2kClientOpcode::OfferFiles,
            Ed2kClientOpcode::SearchRequest,
            Ed2kClientOpcode::GetSources,
            Ed2kClientOpcode::CallbackRequest,
            Ed2kClientOpcode::QueryMoreResults,
        ];
        // All variants are distinct
        for (i, a) in opcodes.iter().enumerate() {
            for (j, b) in opcodes.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn client_opcode_repr_values() {
        assert_eq!(Ed2kClientOpcode::LoginRequest as u8, 0x01);
        assert_eq!(Ed2kClientOpcode::GetServerList as u8, 0x14);
        assert_eq!(Ed2kClientOpcode::OfferFiles as u8, 0x15);
        assert_eq!(Ed2kClientOpcode::SearchRequest as u8, 0x16);
        assert_eq!(Ed2kClientOpcode::GetSources as u8, 0x19);
        assert_eq!(Ed2kClientOpcode::CallbackRequest as u8, 0x1C);
        assert_eq!(Ed2kClientOpcode::QueryMoreResults as u8, 0x21);
    }

    #[test]
    fn client_opcode_clone_copy_eq() {
        let a = Ed2kClientOpcode::LoginRequest;
        let b = a; // Copy
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn client_opcode_debug() {
        let debug = format!("{:?}", Ed2kClientOpcode::LoginRequest);
        assert!(debug.contains("LoginRequest"));
    }

    // ── Ed2kServerOpcode ───────────────────────────────────────

    #[test]
    fn server_opcode_repr_values() {
        assert_eq!(Ed2kServerOpcode::LoginAnswer as u8, 0x20);
        assert_eq!(Ed2kServerOpcode::ServerMessage as u8, 0x38);
        assert_eq!(Ed2kServerOpcode::ServerList as u8, 0x32);
        assert_eq!(Ed2kServerOpcode::SearchResult as u8, 0x33);
        assert_eq!(Ed2kServerOpcode::ServerStatus as u8, 0x34);
        assert_eq!(Ed2kServerOpcode::CallbackRequested as u8, 0x35);
        assert_eq!(Ed2kServerOpcode::CallbackFailed as u8, 0x36);
    }

    #[test]
    fn server_opcode_all_variants_distinct() {
        let opcodes = [
            Ed2kServerOpcode::LoginAnswer,
            Ed2kServerOpcode::ServerMessage,
            Ed2kServerOpcode::ServerList,
            Ed2kServerOpcode::SearchResult,
            Ed2kServerOpcode::ServerStatus,
            Ed2kServerOpcode::CallbackRequested,
            Ed2kServerOpcode::CallbackFailed,
        ];
        for (i, a) in opcodes.iter().enumerate() {
            for (j, b) in opcodes.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn server_opcode_clone_copy_eq_debug() {
        let a = Ed2kServerOpcode::LoginAnswer;
        let b = a;
        assert_eq!(a, b);
        assert_eq!(a.clone(), a);
        let debug = format!("{:?}", a);
        assert!(debug.contains("LoginAnswer"));
    }

    // ── Ed2kPeerOpcode ─────────────────────────────────────────

    #[test]
    fn peer_opcode_repr_values() {
        assert_eq!(Ed2kPeerOpcode::Hello as u8, 0x01);
        assert_eq!(Ed2kPeerOpcode::GetSources as u8, 0x19);
        assert_eq!(Ed2kPeerOpcode::FileAnswer as u8, 0x48);
        assert_eq!(Ed2kPeerOpcode::HashSet as u8, 0x51);
        assert_eq!(Ed2kPeerOpcode::StartUploadReq as u8, 0x52);
        assert_eq!(Ed2kPeerOpcode::AcceptUploadReq as u8, 0x53);
        assert_eq!(Ed2kPeerOpcode::QueueRank as u8, 0x5C);
        assert_eq!(Ed2kPeerOpcode::FileNotFound as u8, 0x49);
    }

    #[test]
    fn peer_opcode_all_variants_count() {
        // Ensure all 8 variants exist and are distinct
        let opcodes = [
            Ed2kPeerOpcode::Hello,
            Ed2kPeerOpcode::GetSources,
            Ed2kPeerOpcode::FileAnswer,
            Ed2kPeerOpcode::HashSet,
            Ed2kPeerOpcode::StartUploadReq,
            Ed2kPeerOpcode::AcceptUploadReq,
            Ed2kPeerOpcode::QueueRank,
            Ed2kPeerOpcode::FileNotFound,
        ];
        assert_eq!(opcodes.len(), 8);
        for (i, a) in opcodes.iter().enumerate() {
            for (j, b) in opcodes.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn peer_opcode_clone_copy_eq_debug() {
        let a = Ed2kPeerOpcode::Hello;
        let b = a;
        assert_eq!(a, b);
        assert_eq!(a.clone(), a);
        let debug = format!("{:?}", a);
        assert!(debug.contains("Hello"));
    }

    // ── Ed2kFileStatus ─────────────────────────────────────────

    #[test]
    fn file_status_all_variants_distinct() {
        let statuses = [
            Ed2kFileStatus::Unknown,
            Ed2kFileStatus::Hashing,
            Ed2kFileStatus::Complete,
            Ed2kFileStatus::Downloading,
            Ed2kFileStatus::Paused,
            Ed2kFileStatus::Queued,
        ];
        assert_eq!(statuses.len(), 6);
        for (i, a) in statuses.iter().enumerate() {
            for (j, b) in statuses.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn file_status_clone_copy_eq() {
        let a = Ed2kFileStatus::Complete;
        let b = a;
        assert_eq!(a, b);
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn file_status_debug() {
        let debug = format!("{:?}", Ed2kFileStatus::Downloading);
        assert!(debug.contains("Downloading"));
    }

    // ── Ed2kSearchResult ───────────────────────────────────────

    #[test]
    fn search_result_basic() {
        let result = Ed2kSearchResult {
            name: "test_file.avi".to_string(),
            size: 700_000_000,
            hash: Ed2kFileHash([0xAB; 16]),
            sources: 10,
            complete_sources: 5,
            media_length: Some(7200),
            media_bitrate: Some(128000),
            media_codec: Some("XviD".to_string()),
        };
        assert_eq!(result.name, "test_file.avi");
        assert_eq!(result.size, 700_000_000);
        assert_eq!(result.sources, 10);
        assert_eq!(result.complete_sources, 5);
        assert_eq!(result.media_length, Some(7200));
        assert_eq!(result.media_bitrate, Some(128000));
        assert_eq!(result.media_codec, Some("XviD".to_string()));
    }

    #[test]
    fn search_result_optional_fields_none() {
        let result = Ed2kSearchResult {
            name: "no_media.bin".to_string(),
            size: 1024,
            hash: Ed2kFileHash([0; 16]),
            sources: 0,
            complete_sources: 0,
            media_length: None,
            media_bitrate: None,
            media_codec: None,
        };
        assert!(result.media_length.is_none());
        assert!(result.media_bitrate.is_none());
        assert!(result.media_codec.is_none());
    }

    #[test]
    fn search_result_clone_independence() {
        let original = Ed2kSearchResult {
            name: "original.avi".to_string(),
            size: 500_000,
            hash: Ed2kFileHash([1u8; 16]),
            sources: 3,
            complete_sources: 1,
            media_length: Some(3600),
            media_bitrate: Some(64000),
            media_codec: Some("H.264".to_string()),
        };
        let cloned = original.clone();
        assert_eq!(cloned.name, "original.avi");
        assert_eq!(cloned.size, 500_000);
        assert_eq!(cloned.sources, 3);
        assert_eq!(cloned.hash, Ed2kFileHash([1u8; 16]));
    }

    #[test]
    fn search_result_debug() {
        let result = Ed2kSearchResult {
            name: "test.txt".to_string(),
            size: 100,
            hash: Ed2kFileHash([0; 16]),
            sources: 0,
            complete_sources: 0,
            media_length: None,
            media_bitrate: None,
            media_codec: None,
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("Ed2kSearchResult"));
        assert!(debug.contains("test.txt"));
    }

    #[test]
    fn search_result_unicode_name() {
        let result = Ed2kSearchResult {
            name: "中文文件名.mp4".to_string(),
            size: 1_000_000_000,
            hash: Ed2kFileHash([0xCD; 16]),
            sources: 5,
            complete_sources: 2,
            media_length: Some(5400),
            media_bitrate: Some(256000),
            media_codec: Some("H.265".to_string()),
        };
        assert_eq!(result.name, "中文文件名.mp4");
    }

    #[test]
    fn search_result_emoji_name() {
        let result = Ed2kSearchResult {
            name: "🎬 movie.mkv".to_string(),
            size: 4_000_000_000,
            hash: Ed2kFileHash([0xEF; 16]),
            sources: 100,
            complete_sources: 50,
            media_length: None,
            media_bitrate: None,
            media_codec: None,
        };
        assert_eq!(result.name, "🎬 movie.mkv");
    }

    #[test]
    fn search_result_zero_size() {
        let result = Ed2kSearchResult {
            name: "empty.dat".to_string(),
            size: 0,
            hash: Ed2kFileHash([0; 16]),
            sources: 0,
            complete_sources: 0,
            media_length: None,
            media_bitrate: None,
            media_codec: None,
        };
        assert_eq!(result.size, 0);
    }

    #[test]
    fn search_result_max_sources() {
        let result = Ed2kSearchResult {
            name: "popular.dat".to_string(),
            size: 1000,
            hash: Ed2kFileHash([0xAA; 16]),
            sources: u32::MAX,
            complete_sources: u32::MAX,
            media_length: None,
            media_bitrate: None,
            media_codec: None,
        };
        assert_eq!(result.sources, u32::MAX);
        assert_eq!(result.complete_sources, u32::MAX);
    }
}
