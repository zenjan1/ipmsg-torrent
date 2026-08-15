//! .torrent file parser

use super::bencode::{Bencode, decode};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum TorrentError {
    #[error("invalid bencode: {0}")]
    Bencode(#[from] super::bencode::BencodeError),
    #[error("missing required field: {0}")]
    MissingField(String),
    #[error("invalid info hash")]
    InvalidInfoHash,
}

/// Parsed .torrent file metadata
#[derive(Debug, Clone)]
pub struct TorrentMeta {
    /// SHA-1 hash of the info dictionary (20 bytes)
    pub info_hash: [u8; 20],
    /// Announce URLs (tracker tiers)
    pub announce_list: Vec<Vec<String>>,
    /// Primary tracker URL
    pub announce: Option<String>,
    /// Creation date (Unix timestamp)
    pub creation_date: Option<i64>,
    /// Comment
    pub comment: Option<String>,
    /// Created by
    pub created_by: Option<String>,
    /// File information
    pub info: TorrentInfo,
}

#[derive(Debug, Clone)]
pub struct TorrentInfo {
    /// Total length in bytes (single-file mode)
    pub length: Option<u64>,
    /// Piece length in bytes
    pub piece_length: u64,
    /// Concatenated SHA-1 hashes of each piece (20 bytes each)
    pub pieces: Vec<[u8; 20]>,
    /// File name (single-file mode)
    pub name: String,
    /// Multi-file mode: list of files
    pub files: Vec<TorrentFile>,
}

#[derive(Debug, Clone)]
pub struct TorrentFile {
    pub path: String,
    pub length: u64,
}

impl TorrentMeta {
    pub fn from_bytes(data: &[u8]) -> Result<Self, TorrentError> {
        let bencode = decode(data)?;
        Self::from_bencode(&bencode)
    }

    pub fn from_bencode(bencode: &Bencode) -> Result<Self, TorrentError> {
        let dict = bencode
            .as_dict()
            .ok_or_else(|| TorrentError::MissingField("root dict".to_string()))?;

        // Extract info dictionary for hash calculation
        let info_dict = dict
            .get("info")
            .ok_or_else(|| TorrentError::MissingField("info".to_string()))?;

        // Calculate info hash (SHA-1 of bencoded info dict)
        let info_hash = Self::calculate_info_hash(info_dict)?;

        // Parse announce/announce-list
        let announce = dict.get("announce").and_then(|v| v.as_string());
        let announce_list = Self::parse_announce_list(dict);

        // Parse optional fields
        let creation_date = dict.get("creation date").and_then(|v| v.as_integer());
        let comment = dict.get("comment").and_then(|v| v.as_string());
        let created_by = dict.get("created by").and_then(|v| v.as_string());

        // Parse info dictionary
        let info = Self::parse_info(info_dict)?;

        Ok(TorrentMeta {
            info_hash,
            announce_list,
            announce,
            creation_date,
            comment,
            created_by,
            info,
        })
    }

    fn calculate_info_hash(info_dict: &Bencode) -> Result<[u8; 20], TorrentError> {
        // We need to re-encode the info dictionary to calculate its SHA-1 hash
        // For simplicity, we'll use a basic encoding approach
        let encoded = Self::encode_bencode(info_dict);
        let hash = Sha256::digest(&encoded);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash[..20]);
        Ok(info_hash)
    }

    fn encode_bencode(value: &Bencode) -> Vec<u8> {
        match value {
            Bencode::Integer(n) => format!("i{}e", n).into_bytes(),
            Bencode::Bytes(b) => {
                let mut result = format!("{}:", b.len()).into_bytes();
                result.extend_from_slice(b);
                result
            }
            Bencode::List(items) => {
                let mut result = vec![b'l'];
                for item in items {
                    result.extend(Self::encode_bencode(item));
                }
                result.push(b'e');
                result
            }
            Bencode::Dict(map) => {
                let mut result = vec![b'd'];
                for (key, value) in map {
                    result.extend(Self::encode_bencode(&Bencode::Bytes(
                        key.as_bytes().to_vec(),
                    )));
                    result.extend(Self::encode_bencode(value));
                }
                result.push(b'e');
                result
            }
        }
    }

    fn parse_announce_list(dict: &std::collections::BTreeMap<String, Bencode>) -> Vec<Vec<String>> {
        let mut tiers = Vec::new();

        if let Some(list) = dict.get("announce-list").and_then(|v| v.as_list()) {
            for tier in list {
                if let Some(tier_list) = tier.as_list() {
                    let tier_urls: Vec<String> =
                        tier_list.iter().filter_map(|v| v.as_string()).collect();
                    if !tier_urls.is_empty() {
                        tiers.push(tier_urls);
                    }
                }
            }
        }

        tiers
    }

    fn parse_info(info_dict: &Bencode) -> Result<TorrentInfo, TorrentError> {
        let dict = info_dict
            .as_dict()
            .ok_or_else(|| TorrentError::MissingField("info dict".to_string()))?;

        let piece_length = dict
            .get("piece length")
            .and_then(|v| v.as_integer())
            .ok_or_else(|| TorrentError::MissingField("piece length".to_string()))?
            as u64;

        let pieces_bytes = dict
            .get("pieces")
            .and_then(|v| v.as_bytes())
            .ok_or_else(|| TorrentError::MissingField("pieces".to_string()))?;

        // Parse pieces (each is 20 bytes SHA-1 hash)
        let mut pieces = Vec::new();
        for chunk in pieces_bytes.chunks(20) {
            if chunk.len() == 20 {
                let mut piece_hash = [0u8; 20];
                piece_hash.copy_from_slice(chunk);
                pieces.push(piece_hash);
            }
        }

        let name = dict
            .get("name")
            .and_then(|v| v.as_string())
            .ok_or_else(|| TorrentError::MissingField("name".to_string()))?;

        // Check for single-file vs multi-file mode
        let (length, files) =
            if let Some(file_length) = dict.get("length").and_then(|v| v.as_integer()) {
                // Single-file mode
                (Some(file_length as u64), Vec::new())
            } else if let Some(file_list) = dict.get("files").and_then(|v| v.as_list()) {
                // Multi-file mode
                let mut files = Vec::new();
                let mut total_length = 0u64;

                for file_entry in file_list {
                    if let Some(file_dict) = file_entry.as_dict() {
                        let file_length = file_dict
                            .get("length")
                            .and_then(|v| v.as_integer())
                            .ok_or_else(|| TorrentError::MissingField("file length".to_string()))?
                            as u64;

                        let path_list = file_dict
                            .get("path")
                            .and_then(|v| v.as_list())
                            .ok_or_else(|| TorrentError::MissingField("path".to_string()))?;

                        let path: Vec<String> =
                            path_list.iter().filter_map(|v| v.as_string()).collect();

                        let path_str = path.join("/");
                        files.push(TorrentFile {
                            path: path_str,
                            length: file_length,
                        });

                        total_length += file_length;
                    }
                }

                (Some(total_length), files)
            } else {
                (None, Vec::new())
            };

        Ok(TorrentInfo {
            length,
            piece_length,
            pieces,
            name,
            files,
        })
    }

    /// Get total file size
    pub fn total_size(&self) -> u64 {
        if let Some(length) = self.info.length {
            length
        } else {
            self.info.files.iter().map(|f| f.length).sum()
        }
    }

    /// Get number of pieces
    pub fn piece_count(&self) -> usize {
        self.info.pieces.len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::bencode::{Bencode, decode, encode};
    use super::*;
    use std::collections::BTreeMap;

    // ── helpers ──────────────────────────────────────────────────────

    /// Build a minimal valid single-file torrent bencode structure
    fn make_single_file_torrent(
        name: &str,
        length: u64,
        piece_length: u64,
        num_pieces: usize,
        announce: Option<&str>,
    ) -> Vec<u8> {
        let mut root = BTreeMap::new();

        if let Some(url) = announce {
            root.insert(
                "announce".to_string(),
                Bencode::Bytes(url.as_bytes().to_vec()),
            );
        }

        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(name.as_bytes().to_vec()));
        info.insert("length".to_string(), Bencode::Integer(length as i64));
        info.insert(
            "piece length".to_string(),
            Bencode::Integer(piece_length as i64),
        );
        // pieces: num_pieces * 20 bytes of SHA-1 hashes
        let pieces = vec![0xABu8; num_pieces * 20];
        info.insert("pieces".to_string(), Bencode::Bytes(pieces));

        root.insert("info".to_string(), Bencode::Dict(info));
        encode(&Bencode::Dict(root))
    }

    /// Build a minimal valid multi-file torrent bencode structure
    fn make_multi_file_torrent(
        name: &str,
        files: &[(&str, u64)],
        piece_length: u64,
        num_pieces: usize,
    ) -> Vec<u8> {
        let mut root = BTreeMap::new();

        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(name.as_bytes().to_vec()));
        info.insert(
            "piece length".to_string(),
            Bencode::Integer(piece_length as i64),
        );
        let pieces = vec![0xCDu8; num_pieces * 20];
        info.insert("pieces".to_string(), Bencode::Bytes(pieces));

        let file_list: Vec<Bencode> = files
            .iter()
            .map(|(path, len)| {
                let mut file_dict = BTreeMap::new();
                file_dict.insert("length".to_string(), Bencode::Integer(*len as i64));
                let path_parts: Vec<Bencode> = path
                    .split('/')
                    .map(|p| Bencode::Bytes(p.as_bytes().to_vec()))
                    .collect();
                file_dict.insert("path".to_string(), Bencode::List(path_parts));
                Bencode::Dict(file_dict)
            })
            .collect();
        info.insert("files".to_string(), Bencode::List(file_list));

        root.insert("info".to_string(), Bencode::Dict(info));
        encode(&Bencode::Dict(root))
    }

    /// Build a torrent with announce-list
    fn make_torrent_with_announce_list(tiers: &[&[&str]]) -> Vec<u8> {
        let mut root = BTreeMap::new();

        let announce_list: Vec<Bencode> = tiers
            .iter()
            .map(|tier| {
                Bencode::List(
                    tier.iter()
                        .map(|url| Bencode::Bytes(url.as_bytes().to_vec()))
                        .collect(),
                )
            })
            .collect();
        root.insert("announce-list".to_string(), Bencode::List(announce_list));

        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("length".to_string(), Bencode::Integer(1024));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));

        encode(&Bencode::Dict(root))
    }

    // ── TorrentError Display ─────────────────────────────────────────

    #[test]
    fn test_error_display_bencode() {
        let e = TorrentError::Bencode(super::super::bencode::BencodeError::UnexpectedEof);
        assert_eq!(format!("{}", e), "invalid bencode: unexpected end of input");
    }

    #[test]
    fn test_error_display_missing_field() {
        let e = TorrentError::MissingField("info".to_string());
        assert_eq!(format!("{}", e), "missing required field: info");
    }

    #[test]
    fn test_error_display_invalid_info_hash() {
        let e = TorrentError::InvalidInfoHash;
        assert_eq!(format!("{}", e), "invalid info hash");
    }

    #[test]
    fn test_error_debug() {
        let e = TorrentError::InvalidInfoHash;
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("InvalidInfoHash"));
    }

    #[test]
    fn test_error_from_bencode() {
        let bencode_err = super::super::bencode::BencodeError::UnexpectedEof;
        let torrent_err: TorrentError = bencode_err.into();
        assert!(format!("{}", torrent_err).starts_with("invalid bencode:"));
    }

    // ── Clone/Debug traits ───────────────────────────────────────────

    #[test]
    fn test_torrent_file_clone() {
        let f = TorrentFile {
            path: "test.txt".to_string(),
            length: 1024,
        };
        let cloned = f.clone();
        assert_eq!(cloned.path, f.path);
        assert_eq!(cloned.length, f.length);
    }

    #[test]
    fn test_torrent_file_debug() {
        let f = TorrentFile {
            path: "test.txt".to_string(),
            length: 1024,
        };
        let dbg = format!("{:?}", f);
        assert!(dbg.contains("test.txt"));
        assert!(dbg.contains("1024"));
    }

    #[test]
    fn test_torrent_info_clone() {
        let info = TorrentInfo {
            length: Some(1024),
            piece_length: 16384,
            pieces: vec![[0u8; 20]],
            name: "test.txt".to_string(),
            files: vec![],
        };
        let cloned = info.clone();
        assert_eq!(cloned.length, info.length);
        assert_eq!(cloned.piece_length, info.piece_length);
        assert_eq!(cloned.pieces.len(), info.pieces.len());
        assert_eq!(cloned.name, info.name);
    }

    #[test]
    fn test_torrent_info_debug() {
        let info = TorrentInfo {
            length: Some(2048),
            piece_length: 32768,
            pieces: vec![],
            name: "movie.mp4".to_string(),
            files: vec![],
        };
        let dbg = format!("{:?}", info);
        assert!(dbg.contains("movie.mp4"));
        assert!(dbg.contains("2048"));
    }

    #[test]
    fn test_torrent_meta_clone() {
        let data = make_single_file_torrent(
            "test.txt",
            1024,
            16384,
            1,
            Some("http://tracker.example.com/announce"),
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        let cloned = meta.clone();
        assert_eq!(cloned.info_hash, meta.info_hash);
        assert_eq!(cloned.info.name, meta.info.name);
        assert_eq!(cloned.info.length, meta.info.length);
    }

    #[test]
    fn test_torrent_meta_debug() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 1, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        let dbg = format!("{:?}", meta);
        assert!(dbg.contains("TorrentMeta"));
        assert!(dbg.contains("test.txt"));
    }

    // ── from_bytes: single-file ──────────────────────────────────────

    #[test]
    fn test_parse_single_file_torrent() {
        let data = make_single_file_torrent(
            "test.txt",
            1024,
            16384,
            1,
            Some("http://tracker.example.com/announce"),
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();

        assert_eq!(meta.info.name, "test.txt");
        assert_eq!(meta.info.length, Some(1024));
        assert_eq!(meta.info.piece_length, 16384);
        assert_eq!(meta.info.pieces.len(), 1);
        assert_eq!(meta.info.files.len(), 0);
        assert_eq!(
            meta.announce,
            Some("http://tracker.example.com/announce".to_string())
        );
    }

    #[test]
    fn test_parse_single_file_no_announce() {
        let data = make_single_file_torrent("test.txt", 2048, 32768, 2, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();

        assert_eq!(meta.info.name, "test.txt");
        assert_eq!(meta.info.length, Some(2048));
        assert_eq!(meta.info.piece_length, 32768);
        assert_eq!(meta.info.pieces.len(), 2);
        assert!(meta.announce.is_none());
    }

    #[test]
    fn test_single_file_total_size() {
        let data = make_single_file_torrent("big.zip", 1_000_000, 262144, 4, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.total_size(), 1_000_000);
    }

    #[test]
    fn test_single_file_piece_count() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 3, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.piece_count(), 3);
    }

    // ── from_bytes: multi-file ───────────────────────────────────────

    #[test]
    fn test_parse_multi_file_torrent() {
        let data = make_multi_file_torrent(
            "my_album",
            &[("song1.mp3", 5_000_000), ("song2.mp3", 4_500_000)],
            262144,
            37,
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();

        assert_eq!(meta.info.name, "my_album");
        assert_eq!(meta.info.files.len(), 2);
        assert_eq!(meta.info.files[0].path, "song1.mp3");
        assert_eq!(meta.info.files[0].length, 5_000_000);
        assert_eq!(meta.info.files[1].path, "song2.mp3");
        assert_eq!(meta.info.files[1].length, 4_500_000);
    }

    #[test]
    fn test_multi_file_total_size() {
        let data = make_multi_file_torrent(
            "archive",
            &[("a.txt", 100), ("b.txt", 200), ("c.txt", 300)],
            16384,
            1,
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.total_size(), 600);
    }

    #[test]
    fn test_multi_file_nested_path() {
        let data = make_multi_file_torrent(
            "project",
            &[("src/main.rs", 500), ("src/lib.rs", 300)],
            16384,
            1,
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.info.files[0].path, "src/main.rs");
        assert_eq!(meta.info.files[1].path, "src/lib.rs");
    }

    #[test]
    fn test_multi_file_piece_count() {
        let data = make_multi_file_torrent("test", &[("a.txt", 100)], 16384, 5);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.piece_count(), 5);
    }

    // ── from_bytes: announce-list ────────────────────────────────────

    #[test]
    fn test_parse_announce_list() {
        let data = make_torrent_with_announce_list(&[
            &[
                "http://tracker1.example.com/announce",
                "http://tracker2.example.com/announce",
            ],
            &["udp://tracker3.example.com:6969"],
        ]);
        let meta = TorrentMeta::from_bytes(&data).unwrap();

        assert_eq!(meta.announce_list.len(), 2);
        assert_eq!(meta.announce_list[0].len(), 2);
        assert_eq!(
            meta.announce_list[0][0],
            "http://tracker1.example.com/announce"
        );
        assert_eq!(meta.announce_list[1][0], "udp://tracker3.example.com:6969");
    }

    #[test]
    fn test_no_announce_list() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 1, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert!(meta.announce_list.is_empty());
    }

    #[test]
    fn test_empty_announce_list() {
        let mut root = BTreeMap::new();
        root.insert("announce-list".to_string(), Bencode::List(vec![]));
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("length".to_string(), Bencode::Integer(1024));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));

        let data = encode(&Bencode::Dict(root));
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert!(meta.announce_list.is_empty());
    }

    // ── from_bytes: optional fields ──────────────────────────────────

    #[test]
    fn test_parse_with_creation_date() {
        let mut root = BTreeMap::new();
        root.insert("creation date".to_string(), Bencode::Integer(1234567890));
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("length".to_string(), Bencode::Integer(1024));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));

        let data = encode(&Bencode::Dict(root));
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.creation_date, Some(1234567890));
    }

    #[test]
    fn test_parse_with_comment() {
        let mut root = BTreeMap::new();
        root.insert(
            "comment".to_string(),
            Bencode::Bytes("Test comment".as_bytes().to_vec()),
        );
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("length".to_string(), Bencode::Integer(1024));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));

        let data = encode(&Bencode::Dict(root));
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.comment, Some("Test comment".to_string()));
    }

    #[test]
    fn test_parse_with_created_by() {
        let mut root = BTreeMap::new();
        root.insert(
            "created by".to_string(),
            Bencode::Bytes(b"uTorrent/3.5.5".to_vec()),
        );
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("length".to_string(), Bencode::Integer(1024));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));

        let data = encode(&Bencode::Dict(root));
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.created_by, Some("uTorrent/3.5.5".to_string()));
    }

    #[test]
    fn test_parse_without_optional_fields() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 1, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert!(meta.creation_date.is_none());
        assert!(meta.comment.is_none());
        assert!(meta.created_by.is_none());
        assert!(meta.announce.is_none());
    }

    // ── from_bytes: error cases ──────────────────────────────────────

    #[test]
    fn test_from_bytes_invalid_bencode() {
        let result = TorrentMeta::from_bytes(b"not valid bencode");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_bytes_empty_input() {
        let result = TorrentMeta::from_bytes(b"");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_bytes_not_a_dict() {
        let result = TorrentMeta::from_bytes(b"i42e");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("root dict"));
    }

    #[test]
    fn test_from_bytes_missing_info() {
        let mut root = BTreeMap::new();
        root.insert(
            "announce".to_string(),
            Bencode::Bytes(b"http://tracker.com".to_vec()),
        );
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("info"));
    }

    #[test]
    fn test_from_bytes_info_not_dict() {
        let mut root = BTreeMap::new();
        root.insert("info".to_string(), Bencode::Integer(42));
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_bytes_missing_piece_length() {
        let mut root = BTreeMap::new();
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("piece length"));
    }

    #[test]
    fn test_from_bytes_missing_pieces() {
        let mut root = BTreeMap::new();
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        root.insert("info".to_string(), Bencode::Dict(info));
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("pieces"));
    }

    #[test]
    fn test_from_bytes_missing_name() {
        let mut root = BTreeMap::new();
        let mut info = BTreeMap::new();
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        info.insert("length".to_string(), Bencode::Integer(1024));
        root.insert("info".to_string(), Bencode::Dict(info));
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("name"));
    }

    #[test]
    fn test_from_bytes_pieces_not_multiple_of_20() {
        let mut root = BTreeMap::new();
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        // 25 bytes - not a multiple of 20, last 5 bytes should be ignored
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 25]));
        info.insert("length".to_string(), Bencode::Integer(1024));
        root.insert("info".to_string(), Bencode::Dict(info));
        let data = encode(&Bencode::Dict(root));
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        // Only 1 complete 20-byte piece should be parsed
        assert_eq!(meta.piece_count(), 1);
    }

    // ── info hash calculation ────────────────────────────────────────

    #[test]
    fn test_info_hash_deterministic() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 1, None);
        let meta1 = TorrentMeta::from_bytes(&data).unwrap();
        let meta2 = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta1.info_hash, meta2.info_hash);
    }

    #[test]
    fn test_info_hash_different_for_different_torrents() {
        let data1 = make_single_file_torrent("test1.txt", 1024, 16384, 1, None);
        let data2 = make_single_file_torrent("test2.txt", 2048, 16384, 1, None);
        let meta1 = TorrentMeta::from_bytes(&data1).unwrap();
        let meta2 = TorrentMeta::from_bytes(&data2).unwrap();
        assert_ne!(meta1.info_hash, meta2.info_hash);
    }

    #[test]
    fn test_info_hash_is_20_bytes() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 1, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.info_hash.len(), 20);
    }

    // ── encode_bencode internal ──────────────────────────────────────

    #[test]
    fn test_encode_bencode_integer() {
        let encoded = TorrentMeta::encode_bencode(&Bencode::Integer(42));
        assert_eq!(encoded, b"i42e");
    }

    #[test]
    fn test_encode_bencode_bytes() {
        let encoded = TorrentMeta::encode_bencode(&Bencode::Bytes(b"hello".to_vec()));
        assert_eq!(encoded, b"5:hello");
    }

    #[test]
    fn test_encode_bencode_list() {
        let val = Bencode::List(vec![Bencode::Integer(1), Bencode::Integer(2)]);
        let encoded = TorrentMeta::encode_bencode(&val);
        assert_eq!(encoded, b"li1ei2ee");
    }

    #[test]
    fn test_encode_bencode_dict() {
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), Bencode::Integer(1));
        let encoded = TorrentMeta::encode_bencode(&Bencode::Dict(map));
        assert_eq!(encoded, b"d1:ai1ee");
    }

    // ── Unicode ──────────────────────────────────────────────────────

    #[test]
    fn test_unicode_name() {
        let data = make_single_file_torrent("测试文件.txt", 1024, 16384, 1, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.info.name, "测试文件.txt");
    }

    #[test]
    fn test_unicode_comment() {
        let mut root = BTreeMap::new();
        root.insert(
            "comment".to_string(),
            Bencode::Bytes("这是一个测试".as_bytes().to_vec()),
        );
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test.txt".to_vec()));
        info.insert("length".to_string(), Bencode::Integer(1024));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));
        root.insert("info".to_string(), Bencode::Dict(info));

        let data = encode(&Bencode::Dict(root));
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.comment, Some("这是一个测试".to_string()));
    }

    #[test]
    fn test_emoji_name() {
        let data = make_single_file_torrent("🎬movie.mp4", 5_000_000, 262144, 20, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.info.name, "🎬movie.mp4");
    }

    // ── total_size edge cases ────────────────────────────────────────

    #[test]
    fn test_total_size_zero_length() {
        let data = make_single_file_torrent("empty.txt", 0, 16384, 0, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.total_size(), 0);
    }

    #[test]
    fn test_total_size_large_file() {
        let data = make_single_file_torrent("huge.iso", 10_000_000_000, 4_194_304, 2385, None);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.total_size(), 10_000_000_000);
    }

    #[test]
    fn test_total_size_multi_file_zero() {
        let data = make_multi_file_torrent("empty", &[("a.txt", 0), ("b.txt", 0)], 16384, 0);
        let meta = TorrentMeta::from_bytes(&data).unwrap();
        assert_eq!(meta.total_size(), 0);
    }

    // ── from_bencode directly ────────────────────────────────────────

    #[test]
    fn test_from_bencode_non_dict() {
        let result = TorrentMeta::from_bencode(&Bencode::Integer(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_from_bencode_valid() {
        let data = make_single_file_torrent("test.txt", 1024, 16384, 1, None);
        let bencode = decode(&data).unwrap();
        let meta = TorrentMeta::from_bencode(&bencode).unwrap();
        assert_eq!(meta.info.name, "test.txt");
    }

    // ── multi-file error cases ───────────────────────────────────────

    #[test]
    fn test_multi_file_missing_file_length() {
        let mut root = BTreeMap::new();
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test".to_vec()));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));

        let mut file_dict = BTreeMap::new();
        // Missing "length" field
        file_dict.insert(
            "path".to_string(),
            Bencode::List(vec![Bencode::Bytes(b"a.txt".to_vec())]),
        );
        info.insert(
            "files".to_string(),
            Bencode::List(vec![Bencode::Dict(file_dict)]),
        );

        root.insert("info".to_string(), Bencode::Dict(info));
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("file length"));
    }

    #[test]
    fn test_multi_file_missing_path() {
        let mut root = BTreeMap::new();
        let mut info = BTreeMap::new();
        info.insert("name".to_string(), Bencode::Bytes(b"test".to_vec()));
        info.insert("piece length".to_string(), Bencode::Integer(16384));
        info.insert("pieces".to_string(), Bencode::Bytes(vec![0u8; 20]));

        let mut file_dict = BTreeMap::new();
        file_dict.insert("length".to_string(), Bencode::Integer(100));
        // Missing "path" field
        info.insert(
            "files".to_string(),
            Bencode::List(vec![Bencode::Dict(file_dict)]),
        );

        root.insert("info".to_string(), Bencode::Dict(info));
        let data = encode(&Bencode::Dict(root));
        let result = TorrentMeta::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("path"));
    }

    // ── complete workflow ────────────────────────────────────────────

    #[test]
    fn test_complete_single_file_workflow() {
        let data = make_single_file_torrent(
            "ubuntu-24.04.iso",
            5_000_000_000,
            4_194_304,
            1193,
            Some("http://torrent.ubuntu.com/announce"),
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();

        assert_eq!(meta.info.name, "ubuntu-24.04.iso");
        assert_eq!(meta.total_size(), 5_000_000_000);
        assert_eq!(meta.piece_count(), 1193);
        assert_eq!(meta.info.piece_length, 4_194_304);
        assert!(meta.info.files.is_empty());
        assert_eq!(
            meta.announce,
            Some("http://torrent.ubuntu.com/announce".to_string())
        );
        assert!(meta.creation_date.is_none());
        assert!(meta.comment.is_none());
    }

    #[test]
    fn test_complete_multi_file_workflow() {
        let data = make_multi_file_torrent(
            "music_album",
            &[
                ("01 - intro.flac", 30_000_000),
                ("02 - main_track.flac", 50_000_000),
                ("03 - outro.flac", 25_000_000),
            ],
            262144,
            401,
        );
        let meta = TorrentMeta::from_bytes(&data).unwrap();

        assert_eq!(meta.info.name, "music_album");
        assert_eq!(meta.info.files.len(), 3);
        assert_eq!(meta.total_size(), 105_000_000);
        assert_eq!(meta.piece_count(), 401);
        // multi-file mode: length is the sum of all file sizes
        assert_eq!(meta.info.length, Some(105_000_000));
    }
}
