//! Magnet link parser and metadata exchange
//!
//! Implements BEP 0009: Extension for Peers to Send Metadata Files
//! Magnet URI format: magnet:?xt=urn:btih:<info-hash>&dn=<name>&tr=<tracker-url>

use url::Url;

/// Parsed magnet link
#[derive(Debug, Clone)]
pub struct MagnetLink {
    /// Info hash (20 bytes for v1, 32 bytes for v2)
    pub info_hash: Vec<u8>,
    /// Display name (optional)
    pub display_name: Option<String>,
    /// Tracker URLs
    pub trackers: Vec<String>,
    /// Peer addresses (x.pe parameter)
    pub peers: Vec<String>,
    /// Is this a v2 magnet link (SHA-256)?
    pub is_v2: bool,
}

impl MagnetLink {
    /// Parse a magnet URI
    pub fn parse(uri: &str) -> Result<Self, MagnetError> {
        let url = Url::parse(uri)
            .map_err(|e| MagnetError::InvalidFormat(format!("Invalid URL: {}", e)))?;

        if url.scheme() != "magnet" {
            return Err(MagnetError::InvalidFormat("Not a magnet link".to_string()));
        }

        let mut info_hash = None;
        let mut display_name = None;
        let mut trackers = Vec::new();
        let mut peers = Vec::new();
        let mut is_v2 = false;

        // Parse query parameters
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "xt" => {
                    // Exact topic: urn:btih:<hash> or urn:btmh:<multihash>
                    if let Some(hash_str) = value.strip_prefix("urn:btih:") {
                        info_hash = Some(Self::parse_info_hash(hash_str, false)?);
                        is_v2 = false;
                    } else if let Some(hash_str) = value.strip_prefix("urn:btmh:") {
                        info_hash = Some(Self::parse_info_hash(hash_str, true)?);
                        is_v2 = true;
                    } else {
                        return Err(MagnetError::InvalidFormat(
                            "Invalid xt parameter (must be urn:btih or urn:btmh)".to_string(),
                        ));
                    }
                }
                "dn" => {
                    display_name = Some(value.to_string());
                }
                "tr" => {
                    trackers.push(value.to_string());
                }
                "x.pe" => {
                    peers.push(value.to_string());
                }
                _ => {
                    // Ignore unknown parameters
                }
            }
        }

        let info_hash = info_hash.ok_or_else(|| {
            MagnetError::InvalidFormat("Missing xt parameter (info hash)".to_string())
        })?;

        Ok(Self {
            info_hash,
            display_name,
            trackers,
            peers,
            is_v2,
        })
    }

    /// Parse info hash from hex or base32 encoding
    fn parse_info_hash(hash_str: &str, is_v2: bool) -> Result<Vec<u8>, MagnetError> {
        // Try hex first (40 chars for v1, 64 chars for v2)
        let expected_len = if is_v2 { 64 } else { 40 };

        if hash_str.len() == expected_len {
            // Hex encoded
            hex::decode(hash_str)
                .map_err(|e| MagnetError::InvalidFormat(format!("Invalid hex hash: {}", e)))
        } else if hash_str.len() == 32 && !is_v2 {
            // Base32 encoded (v1 only)
            Self::decode_base32(hash_str)
        } else {
            Err(MagnetError::InvalidFormat(format!(
                "Invalid hash length: {} (expected {} for {})",
                hash_str.len(),
                expected_len,
                if is_v2 { "v2" } else { "v1" }
            )))
        }
    }

    /// Decode base32 string to bytes
    fn decode_base32(input: &str) -> Result<Vec<u8>, MagnetError> {
        const BASE32_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

        let input = input.to_uppercase();
        let mut output = Vec::new();
        let mut buffer = 0u64;
        let mut bits_left = 0;

        for c in input.chars() {
            if c == '=' {
                break; // Padding
            }

            let value = BASE32_CHARS
                .iter()
                .position(|&b| b == c as u8)
                .ok_or_else(|| MagnetError::InvalidFormat(format!("Invalid base32 char: {}", c)))?;

            buffer = (buffer << 5) | value as u64;
            bits_left += 5;

            if bits_left >= 8 {
                bits_left -= 8;
                output.push((buffer >> bits_left) as u8);
                buffer &= (1 << bits_left) - 1;
            }
        }

        Ok(output)
    }

    /// Convert to a magnet URI string
    pub fn to_uri(&self) -> String {
        let mut uri = String::from("magnet:?");

        // Add xt parameter
        let xt_prefix = if self.is_v2 { "urn:btmh:" } else { "urn:btih:" };
        uri.push_str("xt=");
        uri.push_str(xt_prefix);

        if self.is_v2 {
            uri.push_str(&hex::encode(&self.info_hash));
        } else {
            // Try hex first, fall back to base32 if needed
            uri.push_str(&hex::encode(&self.info_hash));
        }

        // Add optional parameters
        if let Some(ref dn) = self.display_name {
            uri.push_str("&dn=");
            uri.push_str(&urlencoding::encode(dn));
        }

        for tr in &self.trackers {
            uri.push_str("&tr=");
            uri.push_str(&urlencoding::encode(tr));
        }

        for peer in &self.peers {
            uri.push_str("&x.pe=");
            uri.push_str(peer);
        }

        uri
    }
}

/// Magnet link parsing errors
#[derive(Debug, thiserror::Error)]
pub enum MagnetError {
    #[error("Invalid magnet link format: {0}")]
    InvalidFormat(String),
    #[error("Unsupported feature: {0}")]
    Unsupported(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_magnet_v1_hex() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test&tr=http://tracker.example.com/announce";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        assert_eq!(magnet.display_name, Some("test".to_string()));
        assert_eq!(magnet.trackers.len(), 1);
        assert_eq!(magnet.trackers[0], "http://tracker.example.com/announce");
        assert!(!magnet.is_v2);
    }

    #[test]
    fn test_parse_magnet_v1_base32() {
        // 32-char base32 = 20 bytes (160 bits)
        let uri = "magnet:?xt=urn:btih:GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        assert!(!magnet.is_v2);
    }

    #[test]
    fn test_parse_magnet_multiple_trackers() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&tr=http://tracker1.com&tr=http://tracker2.com";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.trackers.len(), 2);
    }

    #[test]
    fn test_parse_magnet_with_peers() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&x.pe=192.168.1.1:6881&x.pe=192.168.1.2:6881";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.peers.len(), 2);
        assert_eq!(magnet.peers[0], "192.168.1.1:6881");
    }

    #[test]
    fn test_to_uri() {
        let magnet = MagnetLink {
            info_hash: vec![
                0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60,
                0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
            ],
            display_name: Some("test file".to_string()),
            trackers: vec!["http://tracker.example.com/announce".to_string()],
            peers: vec![],
            is_v2: false,
        };

        let uri = magnet.to_uri();
        assert!(uri.starts_with("magnet:?xt=urn:btih:"));
        assert!(uri.contains("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
        assert!(uri.contains("test%20file"));
    }

    #[test]
    fn test_invalid_magnet() {
        assert!(MagnetLink::parse("http://example.com").is_err());
        assert!(MagnetLink::parse("magnet:?dn=test").is_err()); // Missing xt
        assert!(MagnetLink::parse("magnet:?xt=invalid").is_err());
    }

    // ===== v2 Magnet Link Parsing =====

    #[test]
    fn test_parse_magnet_v2_hex() {
        // v2 uses btmh prefix and 64-char SHA-256 hash
        let uri =
            "magnet:?xt=urn:btmh:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 32);
        assert!(magnet.is_v2);
        assert!(magnet.display_name.is_none());
        assert!(magnet.trackers.is_empty());
    }

    #[test]
    fn test_parse_magnet_v2_with_all_params() {
        let uri = "magnet:?xt=urn:btmh:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&dn=v2file&tr=http://t1.com&tr=http://t2.com&x.pe=1.2.3.4:6881";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 32);
        assert!(magnet.is_v2);
        assert_eq!(magnet.display_name, Some("v2file".to_string()));
        assert_eq!(magnet.trackers.len(), 2);
        assert_eq!(magnet.peers.len(), 1);
    }

    #[test]
    fn test_v2_hash_length_validation() {
        // Too short for v2 (40 chars instead of 64)
        let uri = "magnet:?xt=urn:btmh:da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
    }

    // ===== Base32 Decoding =====

    #[test]
    fn test_base32_all_zeros() {
        // 32 A's in base32 = 20 zero bytes
        let uri = "magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        assert!(magnet.info_hash.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_base32_lowercase() {
        // Base32 should be case-insensitive
        let uri_upper = "magnet:?xt=urn:btih:GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";
        let uri_lower = "magnet:?xt=urn:btih:gezdgnbvgy3tqojqgezdgnbvgy3tqojq";

        let magnet_upper = MagnetLink::parse(uri_upper).unwrap();
        let magnet_lower = MagnetLink::parse(uri_lower).unwrap();

        assert_eq!(magnet_upper.info_hash, magnet_lower.info_hash);
    }

    #[test]
    fn test_base32_with_padding() {
        // Base32 with = padding becomes 36 chars, which fails length check (expects 32)
        // This is expected behavior - the parser validates length before decoding
        let uri = "magnet:?xt=urn:btih:GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ====";
        let result = MagnetLink::parse(uri);
        assert!(
            result.is_err(),
            "Base32 with padding should fail length validation"
        );
    }

    #[test]
    fn test_invalid_base32_chars() {
        // Base32 only allows A-Z and 2-7, so '1' is invalid
        let uri = "magnet:?xt=urn:btih:11111111111111111111111111111111";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
    }

    // ===== Hash Length Validation =====

    #[test]
    fn test_v1_hash_too_short() {
        // 20 chars instead of 40
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_v1_hash_too_long() {
        // 60 chars instead of 40
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709da39a3ee5e6b4b0d3255bfef9560";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_hex_chars() {
        // 40 chars but contains invalid hex char 'g'
        let uri = "magnet:?xt=urn:btih:gg39a3ee5e6b4b0d3255bfef95601890afd80709";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
    }

    // ===== Parameter Parsing =====

    #[test]
    fn test_parse_only_xt_parameter() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        assert!(magnet.display_name.is_none());
        assert!(magnet.trackers.is_empty());
        assert!(magnet.peers.is_empty());
        assert!(!magnet.is_v2);
    }

    #[test]
    fn test_parse_with_display_name() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=My%20File.txt";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.display_name, Some("My File.txt".to_string()));
    }

    #[test]
    fn test_parse_with_unicode_display_name() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=%E6%96%87%E4%BB%B6.txt";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.display_name, Some("文件.txt".to_string()));
    }

    #[test]
    fn test_parse_many_trackers() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&tr=http://t1.com&tr=http://t2.com&tr=http://t3.com&tr=http://t4.com&tr=http://t5.com";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.trackers.len(), 5);
        assert_eq!(magnet.trackers[0], "http://t1.com");
        assert_eq!(magnet.trackers[4], "http://t5.com");
    }

    #[test]
    fn test_parse_many_peers() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&x.pe=1.1.1.1:6881&x.pe=2.2.2.2:6882&x.pe=3.3.3.3:6883";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.peers.len(), 3);
        assert_eq!(magnet.peers[0], "1.1.1.1:6881");
        assert_eq!(magnet.peers[2], "3.3.3.3:6883");
    }

    #[test]
    fn test_parse_ignores_unknown_params() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&unknown=value&kt=some+keywords&xl=12345";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        // Unknown params should be silently ignored
    }

    #[test]
    fn test_parse_params_in_different_order() {
        let uri1 =
            "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test&tr=http://t.com";
        let uri2 =
            "magnet:?dn=test&tr=http://t.com&xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709";

        let magnet1 = MagnetLink::parse(uri1).unwrap();
        let magnet2 = MagnetLink::parse(uri2).unwrap();

        assert_eq!(magnet1.info_hash, magnet2.info_hash);
        assert_eq!(magnet1.display_name, magnet2.display_name);
        assert_eq!(magnet1.trackers, magnet2.trackers);
    }

    // ===== Error Cases =====

    #[test]
    fn test_not_magnet_scheme() {
        // http:// is a valid URL but not a magnet link
        // The URL parser succeeds, then we check the scheme
        let uri = "http://example.com/?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Not a magnet link"));
    }

    #[test]
    fn test_invalid_xt_prefix() {
        let uri = "magnet:?xt=urn:sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("urn:btih"));
    }

    #[test]
    fn test_missing_xt_value() {
        let uri = "magnet:?xt=";
        let result = MagnetLink::parse(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_uri() {
        let result = MagnetLink::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_garbage_uri() {
        let result = MagnetLink::parse("not a valid uri at all {{{}}}");
        assert!(result.is_err());
    }

    // ===== to_uri Generation =====

    #[test]
    fn test_to_uri_minimal() {
        let magnet = MagnetLink {
            info_hash: vec![
                0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60,
                0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
            ],
            display_name: None,
            trackers: vec![],
            peers: vec![],
            is_v2: false,
        };

        let uri = magnet.to_uri();
        assert!(uri.starts_with("magnet:?xt=urn:btih:"));
        assert!(uri.contains("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
        assert!(!uri.contains("&dn="));
        assert!(!uri.contains("&tr="));
        assert!(!uri.contains("&x.pe="));
    }

    #[test]
    fn test_to_uri_with_display_name() {
        let magnet = MagnetLink {
            info_hash: vec![0; 20],
            display_name: Some("test file.txt".to_string()),
            trackers: vec![],
            peers: vec![],
            is_v2: false,
        };

        let uri = magnet.to_uri();
        assert!(uri.contains("&dn=test%20file.txt"));
    }

    #[test]
    fn test_to_uri_with_trackers() {
        let magnet = MagnetLink {
            info_hash: vec![0; 20],
            display_name: None,
            trackers: vec![
                "http://tracker1.com".to_string(),
                "http://tracker2.com".to_string(),
            ],
            peers: vec![],
            is_v2: false,
        };

        let uri = magnet.to_uri();
        assert!(uri.contains("&tr=http%3A%2F%2Ftracker1.com"));
        assert!(uri.contains("&tr=http%3A%2F%2Ftracker2.com"));
    }

    #[test]
    fn test_to_uri_with_peers() {
        let magnet = MagnetLink {
            info_hash: vec![0; 20],
            display_name: None,
            trackers: vec![],
            peers: vec!["192.168.1.1:6881".to_string()],
            is_v2: false,
        };

        let uri = magnet.to_uri();
        assert!(uri.contains("&x.pe=192.168.1.1:6881"));
    }

    #[test]
    fn test_to_uri_v2() {
        let magnet = MagnetLink {
            info_hash: vec![0xAA; 32],
            display_name: None,
            trackers: vec![],
            peers: vec![],
            is_v2: true,
        };

        let uri = magnet.to_uri();
        assert!(uri.starts_with("magnet:?xt=urn:btmh:"));
        assert!(uri.contains(&"aa".repeat(32)));
    }

    #[test]
    fn test_to_uri_all_params() {
        let magnet = MagnetLink {
            info_hash: vec![0x12; 20],
            display_name: Some("file.mp4".to_string()),
            trackers: vec!["http://t.com".to_string()],
            peers: vec!["1.2.3.4:6881".to_string()],
            is_v2: false,
        };

        let uri = magnet.to_uri();
        assert!(uri.contains("xt=urn:btih:"));
        assert!(uri.contains("&dn=file.mp4"));
        assert!(uri.contains("&tr="));
        assert!(uri.contains("&x.pe="));
    }

    // ===== Roundtrip =====

    #[test]
    fn test_parse_to_uri_roundtrip() {
        let original_uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test&tr=http%3A%2F%2Ftracker.com";
        let magnet = MagnetLink::parse(original_uri).unwrap();
        let generated_uri = magnet.to_uri();

        // Parse the generated URI again
        let magnet2 = MagnetLink::parse(&generated_uri).unwrap();

        assert_eq!(magnet.info_hash, magnet2.info_hash);
        assert_eq!(magnet.display_name, magnet2.display_name);
        assert_eq!(magnet.trackers, magnet2.trackers);
    }

    #[test]
    fn test_to_uri_parse_roundtrip_v2() {
        let magnet = MagnetLink {
            info_hash: vec![0xBB; 32],
            display_name: Some("v2 file".to_string()),
            trackers: vec!["http://t.com".to_string()],
            peers: vec![],
            is_v2: true,
        };

        let uri = magnet.to_uri();
        let parsed = MagnetLink::parse(&uri).unwrap();

        assert_eq!(parsed.info_hash, magnet.info_hash);
        assert_eq!(parsed.display_name, magnet.display_name);
        assert_eq!(parsed.trackers, magnet.trackers);
        assert!(parsed.is_v2);
    }

    // ===== Error Display =====

    #[test]
    fn test_error_display_invalid_format() {
        let err = MagnetError::InvalidFormat("bad format".to_string());
        assert_eq!(err.to_string(), "Invalid magnet link format: bad format");
    }

    #[test]
    fn test_error_display_unsupported() {
        let err = MagnetError::Unsupported("feature X".to_string());
        assert_eq!(err.to_string(), "Unsupported feature: feature X");
    }

    #[test]
    fn test_error_debug() {
        let err = MagnetError::InvalidFormat("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("InvalidFormat"));
    }

    // ===== Traits =====

    #[test]
    fn test_magnet_link_clone() {
        let magnet = MagnetLink {
            info_hash: vec![0xAA; 20],
            display_name: Some("test".to_string()),
            trackers: vec!["http://t.com".to_string()],
            peers: vec!["1.2.3.4:6881".to_string()],
            is_v2: false,
        };

        let cloned = magnet.clone();
        assert_eq!(cloned.info_hash, magnet.info_hash);
        assert_eq!(cloned.display_name, magnet.display_name);
        assert_eq!(cloned.trackers, magnet.trackers);
        assert_eq!(cloned.peers, magnet.peers);
        assert_eq!(cloned.is_v2, magnet.is_v2);
    }

    #[test]
    fn test_magnet_link_debug() {
        let magnet = MagnetLink {
            info_hash: vec![0; 20],
            display_name: None,
            trackers: vec![],
            peers: vec![],
            is_v2: false,
        };

        let debug = format!("{:?}", magnet);
        assert!(debug.contains("MagnetLink"));
        assert!(debug.contains("info_hash"));
    }

    // ===== Edge Cases =====

    #[test]
    fn test_all_zeros_hash() {
        let uri = "magnet:?xt=urn:btih:0000000000000000000000000000000000000000";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        assert!(magnet.info_hash.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_all_ff_hash() {
        let uri = "magnet:?xt=urn:btih:ffffffffffffffffffffffffffffffffffffffff";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
        assert!(magnet.info_hash.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_mixed_case_hex() {
        let uri = "magnet:?xt=urn:btih:Da39A3Ee5e6B4B0d3255BfEf95601890AFd80709";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.info_hash.len(), 20);
    }

    #[test]
    fn test_empty_display_name() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.display_name, Some("".to_string()));
    }

    #[test]
    fn test_special_chars_in_tracker() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&tr=http%3A%2F%2Ftracker.com%2Fannounce%3Fpass%3Dsecret";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.trackers.len(), 1);
        assert!(magnet.trackers[0].contains("tracker.com"));
    }

    #[test]
    fn test_ipv6_peer() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&x.pe=[::1]:6881";
        let magnet = MagnetLink::parse(uri).unwrap();

        assert_eq!(magnet.peers.len(), 1);
        assert_eq!(magnet.peers[0], "[::1]:6881");
    }
}
