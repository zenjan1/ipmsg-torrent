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
            info_hash: vec![0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09],
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
}
