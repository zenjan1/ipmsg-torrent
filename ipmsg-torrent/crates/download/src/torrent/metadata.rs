//! BEP 0009: Extension for Peers to Send Metadata Files
//!
//! This module implements the metadata exchange protocol that allows
//! downloading torrent metadata from peers, enabling magnet link support.

use super::bencode::{Bencode, encode};
use super::peer::{PeerConnection, PeerError};

use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;

/// Metadata piece size (16KB)
#[allow(dead_code)]
const METADATA_PIECE_SIZE: usize = 16 * 1024;

/// Metadata exchange error
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("peer error: {0}")]
    Peer(#[from] PeerError),
    #[error("bencode error: {0}")]
    Bencode(String),
    #[error("metadata verification failed")]
    VerificationFailed,
    #[error("peer does not support metadata exchange")]
    NotSupported,
    #[error("timeout")]
    Timeout,
    #[error("incomplete metadata")]
    Incomplete,
}

/// Metadata fetcher for magnet links
pub struct MetadataFetcher {
    info_hash: [u8; 20],
    metadata_pieces: HashMap<usize, Vec<u8>>,
    total_size: Option<usize>,
}

impl MetadataFetcher {
    /// Create a new metadata fetcher for the given info hash
    pub fn new(info_hash: [u8; 20]) -> Self {
        Self {
            info_hash,
            metadata_pieces: HashMap::new(),
            total_size: None,
        }
    }

    /// Fetch metadata from a peer
    pub async fn fetch_from_peer(
        &mut self,
        addr: SocketAddr,
        peer_id: [u8; 20],
    ) -> Result<Vec<u8>, MetadataError> {
        tracing::info!(addr = %addr, "Connecting to peer for metadata exchange");

        // Connect to peer
        let mut conn = timeout(
            Duration::from_secs(10),
            PeerConnection::connect(addr, self.info_hash, peer_id),
        )
        .await
        .map_err(|_| MetadataError::Timeout)?
        .map_err(MetadataError::Peer)?;

        // Perform extended handshake to check for ut_metadata support
        let ut_metadata_id = self.perform_extended_handshake(&mut conn).await?;

        if ut_metadata_id.is_none() {
            return Err(MetadataError::NotSupported);
        }

        let ut_metadata_id = ut_metadata_id.unwrap();
        tracing::debug!(extension_id = ut_metadata_id, "Peer supports ut_metadata");

        // Request metadata pieces
        self.fetch_all_pieces(&mut conn, ut_metadata_id).await?;

        // Assemble and verify metadata
        let metadata_bytes = self.assemble_metadata()?;
        self.verify_metadata(&metadata_bytes)?;

        Ok(metadata_bytes)
    }

    /// Perform extended handshake (BEP 0010)
    async fn perform_extended_handshake(
        &mut self,
        conn: &mut PeerConnection,
    ) -> Result<Option<u8>, MetadataError> {
        // Send extended handshake
        let mut handshake = Bencode::Dict(std::collections::BTreeMap::new());
        if let Bencode::Dict(ref mut map) = handshake {
            map.insert(
                "m".to_string(),
                Bencode::Dict(std::collections::BTreeMap::new()),
            );
            map.insert(
                "v".to_string(),
                Bencode::Bytes("IPMsg-Torrent/1.0".as_bytes().to_vec()),
            );
            map.insert("reqq".to_string(), Bencode::Integer(256));
        }

        let handshake_bytes = encode(&handshake);

        // Extended message format: msg_id=20, ext_id=0, payload
        let mut ext_msg = vec![0u8; 6];
        ext_msg[0] = 20; // Extended message ID
        ext_msg[1] = 0; // Extended handshake
        ext_msg[2..6].copy_from_slice(&(handshake_bytes.len() as u32).to_be_bytes());
        ext_msg.extend_from_slice(&handshake_bytes);

        conn.send_raw(&ext_msg).await?;

        // Receive extended handshake response
        let response = timeout(Duration::from_secs(5), self.recv_extended_message(conn))
            .await
            .map_err(|_| MetadataError::Timeout)?
            .map_err(MetadataError::Peer)?;

        // Parse response to find ut_metadata extension ID
        if response.len() < 2 || response[0] != 20 || response[1] != 0 {
            return Ok(None);
        }

        let payload = &response[6..];
        let bencode =
            super::bencode::decode(payload).map_err(|e| MetadataError::Bencode(e.to_string()))?;

        if let Bencode::Dict(map) = bencode
            && let Some(Bencode::Dict(extensions)) = map.get("m")
            && let Some(Bencode::Integer(id)) = extensions.get("ut_metadata")
        {
            return Ok(Some(*id as u8));
        }

        Ok(None)
    }

    /// Fetch all metadata pieces from peer
    async fn fetch_all_pieces(
        &mut self,
        conn: &mut PeerConnection,
        ut_metadata_id: u8,
    ) -> Result<(), MetadataError> {
        let mut piece_index = 0;
        let mut consecutive_rejects = 0;

        loop {
            // Request next piece
            let mut request = Bencode::Dict(std::collections::BTreeMap::new());
            if let Bencode::Dict(ref mut map) = request {
                map.insert("msg_type".to_string(), Bencode::Integer(0)); // Request
                map.insert("piece".to_string(), Bencode::Integer(piece_index as i64));
            }

            let request_bytes = encode(&request);

            // Metadata message format: msg_id=20, ext_id=ut_metadata_id, payload
            let mut ext_msg = vec![0u8; 6];
            ext_msg[0] = 20;
            ext_msg[1] = ut_metadata_id;
            ext_msg[2..6].copy_from_slice(&(request_bytes.len() as u32).to_be_bytes());
            ext_msg.extend_from_slice(&request_bytes);

            conn.send_raw(&ext_msg).await?;

            // Receive response
            let response = timeout(Duration::from_secs(5), self.recv_extended_message(conn))
                .await
                .map_err(|_| MetadataError::Timeout)?
                .map_err(MetadataError::Peer)?;

            if response.len() < 6 || response[0] != 20 || response[1] != ut_metadata_id {
                continue;
            }

            let payload = &response[6..];

            // Parse message
            let bencode = super::bencode::decode(payload)
                .map_err(|e| MetadataError::Bencode(e.to_string()))?;

            if let Bencode::Dict(map) = bencode {
                let msg_type = map
                    .get("msg_type")
                    .and_then(|v| v.as_integer())
                    .ok_or_else(|| MetadataError::Bencode("missing msg_type".to_string()))?;

                match msg_type as u8 {
                    0 => {
                        // Request (shouldn't receive this)
                        continue;
                    }
                    1 => {
                        // Data
                        let total_size = map
                            .get("total_size")
                            .and_then(|v| v.as_integer())
                            .map(|v| v as usize);

                        if let Some(size) = total_size {
                            self.total_size = Some(size);
                        }

                        // Find where the bencode ends and binary data begins
                        let bencode_str = encode(&Bencode::Dict(map));
                        let data_start = bencode_str.len();

                        if data_start < payload.len() {
                            let piece_data = payload[data_start..].to_vec();
                            self.metadata_pieces.insert(piece_index, piece_data);
                            tracing::debug!(piece = piece_index, "Received metadata piece");
                        }

                        // Check if we have all pieces
                        if let Some(total) = self.total_size {
                            let received_size: usize =
                                self.metadata_pieces.values().map(|v| v.len()).sum();

                            if received_size >= total {
                                return Ok(());
                            }
                        }

                        piece_index += 1;
                        consecutive_rejects = 0;
                    }
                    2 => {
                        // Reject
                        tracing::warn!(piece = piece_index, "Peer rejected metadata request");
                        consecutive_rejects += 1;

                        if consecutive_rejects > 3 {
                            return Err(MetadataError::Incomplete);
                        }

                        piece_index += 1;
                    }
                    _ => {
                        return Err(MetadataError::Bencode(format!(
                            "unknown msg_type: {}",
                            msg_type
                        )));
                    }
                }
            }
        }
    }

    /// Receive an extended message from peer
    async fn recv_extended_message(&self, conn: &mut PeerConnection) -> Result<Vec<u8>, PeerError> {
        // Read message length (4 bytes)
        let mut len_buf = [0u8; 4];
        conn.read_exact(&mut len_buf).await?;
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len == 0 {
            return Ok(vec![]);
        }

        // Read message payload
        let mut payload = vec![0u8; msg_len];
        conn.read_exact(&mut payload).await?;

        Ok(payload)
    }

    /// Assemble metadata pieces into complete metadata
    fn assemble_metadata(&self) -> Result<Vec<u8>, MetadataError> {
        let total_size = self.total_size.ok_or(MetadataError::Incomplete)?;

        let mut metadata = Vec::with_capacity(total_size);

        for i in 0.. {
            if let Some(piece) = self.metadata_pieces.get(&i) {
                metadata.extend_from_slice(piece);
            } else {
                break;
            }

            if metadata.len() >= total_size {
                break;
            }
        }

        if metadata.len() != total_size {
            return Err(MetadataError::Incomplete);
        }

        Ok(metadata)
    }

    /// Verify metadata by checking info hash
    fn verify_metadata(&self, metadata: &[u8]) -> Result<(), MetadataError> {
        let hash = Sha1::digest(metadata);
        let mut computed_hash = [0u8; 20];
        computed_hash.copy_from_slice(&hash);

        if computed_hash != self.info_hash {
            tracing::error!(
                expected = hex::encode(self.info_hash),
                computed = hex::encode(computed_hash),
                "Metadata verification failed"
            );
            return Err(MetadataError::VerificationFailed);
        }

        tracing::info!("Metadata verified successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Constants ──

    #[test]
    fn test_metadata_piece_size_value() {
        assert_eq!(METADATA_PIECE_SIZE, 16 * 1024);
        assert_eq!(METADATA_PIECE_SIZE, 16384);
    }

    // ── MetadataError Display ──

    #[test]
    fn test_error_display_peer() {
        let err = MetadataError::Peer(PeerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        )));
        let msg = format!("{err}");
        assert!(msg.contains("peer error"), "got: {msg}");
    }

    #[test]
    fn test_error_display_bencode() {
        let err = MetadataError::Bencode("invalid data".to_string());
        let msg = format!("{err}");
        assert!(msg.contains("bencode error"), "got: {msg}");
        assert!(msg.contains("invalid data"), "got: {msg}");
    }

    #[test]
    fn test_error_display_verification_failed() {
        let err = MetadataError::VerificationFailed;
        let msg = format!("{err}");
        assert!(msg.contains("verification failed"), "got: {msg}");
    }

    #[test]
    fn test_error_display_not_supported() {
        let err = MetadataError::NotSupported;
        let msg = format!("{err}");
        assert!(msg.contains("does not support"), "got: {msg}");
    }

    #[test]
    fn test_error_display_timeout() {
        let err = MetadataError::Timeout;
        let msg = format!("{err}");
        assert!(msg.contains("timeout"), "got: {msg}");
    }

    #[test]
    fn test_error_display_incomplete() {
        let err = MetadataError::Incomplete;
        let msg = format!("{err}");
        assert!(msg.contains("incomplete"), "got: {msg}");
    }

    // ── MetadataError Debug ──

    #[test]
    fn test_error_debug_peer() {
        let err = MetadataError::Peer(PeerError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "test",
        )));
        let dbg = format!("{err:?}");
        assert!(dbg.contains("Peer"), "got: {dbg}");
    }

    #[test]
    fn test_error_debug_bencode() {
        let err = MetadataError::Bencode("bad".to_string());
        let dbg = format!("{err:?}");
        assert!(dbg.contains("Bencode"), "got: {dbg}");
        assert!(dbg.contains("bad"), "got: {dbg}");
    }

    #[test]
    fn test_error_debug_all_variants() {
        let errors: Vec<MetadataError> = vec![
            MetadataError::Peer(PeerError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "x",
            ))),
            MetadataError::Bencode("e".into()),
            MetadataError::VerificationFailed,
            MetadataError::NotSupported,
            MetadataError::Timeout,
            MetadataError::Incomplete,
        ];
        for err in &errors {
            let dbg = format!("{err:?}");
            assert!(!dbg.is_empty());
        }
    }

    // ── MetadataError From<PeerError> ──

    #[test]
    fn test_error_from_peer_error() {
        let peer_err = PeerError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
        let err: MetadataError = peer_err.into();
        let msg = format!("{err}");
        assert!(msg.contains("peer error"), "got: {msg}");
    }

    #[test]
    fn test_error_from_peer_preserves_kind() {
        let peer_err = PeerError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        ));
        let err: MetadataError = peer_err.into();
        match err {
            MetadataError::Peer(PeerError::Io(ref e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::TimedOut);
            }
            _ => panic!("expected Peer variant"),
        }
    }

    // ── MetadataError traits ──

    #[test]
    fn test_error_is_std_error() {
        let err = MetadataError::Timeout;
        // Verify it implements std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_error_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<MetadataError>();
        assert_sync::<MetadataError>();
    }

    // ── MetadataFetcher::new ──

    #[test]
    fn test_metadata_fetcher_creation() {
        let info_hash = [0u8; 20];
        let fetcher = MetadataFetcher::new(info_hash);
        assert_eq!(fetcher.info_hash, info_hash);
        assert!(fetcher.metadata_pieces.is_empty());
        assert!(fetcher.total_size.is_none());
    }

    #[test]
    fn test_metadata_fetcher_all_zeros_hash() {
        let info_hash = [0u8; 20];
        let fetcher = MetadataFetcher::new(info_hash);
        assert_eq!(fetcher.info_hash, [0u8; 20]);
    }

    #[test]
    fn test_metadata_fetcher_all_ff_hash() {
        let info_hash = [0xFFu8; 20];
        let fetcher = MetadataFetcher::new(info_hash);
        assert_eq!(fetcher.info_hash, [0xFFu8; 20]);
    }

    #[test]
    fn test_metadata_fetcher_custom_hash() {
        let mut info_hash = [0u8; 20];
        for (i, b) in info_hash.iter_mut().enumerate() {
            *b = i as u8;
        }
        let fetcher = MetadataFetcher::new(info_hash);
        assert_eq!(fetcher.info_hash, info_hash);
        assert!(fetcher.metadata_pieces.is_empty());
        assert!(fetcher.total_size.is_none());
    }

    #[test]
    fn test_metadata_fetcher_initial_state() {
        let fetcher = MetadataFetcher::new([1u8; 20]);
        // No pieces collected
        assert!(fetcher.metadata_pieces.is_empty());
        // No total size known
        assert!(fetcher.total_size.is_none());
        // Info hash preserved
        assert_eq!(fetcher.info_hash, [1u8; 20]);
    }

    #[test]
    fn test_metadata_fetcher_independent_instances() {
        let f1 = MetadataFetcher::new([0u8; 20]);
        let f2 = MetadataFetcher::new([1u8; 20]);
        assert_ne!(f1.info_hash, f2.info_hash);
    }

    // ── assemble_metadata ──

    #[test]
    fn test_assemble_metadata_no_total_size() {
        let fetcher = MetadataFetcher::new([0u8; 20]);
        let result = fetcher.assemble_metadata();
        assert!(result.is_err());
        match result.unwrap_err() {
            MetadataError::Incomplete => {}
            other => panic!("expected Incomplete, got: {other:?}"),
        }
    }

    #[test]
    fn test_assemble_metadata_empty_pieces() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        fetcher.total_size = Some(100);
        // No pieces inserted
        let result = fetcher.assemble_metadata();
        assert!(result.is_err());
        match result.unwrap_err() {
            MetadataError::Incomplete => {}
            other => panic!("expected Incomplete, got: {other:?}"),
        }
    }

    #[test]
    fn test_assemble_metadata_single_piece() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        let data = vec![42u8; 100];
        fetcher.total_size = Some(100);
        fetcher.metadata_pieces.insert(0, data.clone());
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_assemble_metadata_multiple_pieces() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        let piece0 = vec![1u8; 50];
        let piece1 = vec![2u8; 50];
        fetcher.total_size = Some(100);
        fetcher.metadata_pieces.insert(0, piece0);
        fetcher.metadata_pieces.insert(1, piece1);
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        let assembled = result.unwrap();
        assert_eq!(assembled.len(), 100);
        assert!(assembled[..50].iter().all(|&b| b == 1));
        assert!(assembled[50..].iter().all(|&b| b == 2));
    }

    #[test]
    fn test_assemble_metadata_gap_in_pieces() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        fetcher.total_size = Some(150);
        fetcher.metadata_pieces.insert(0, vec![1u8; 50]);
        // Missing piece 1
        fetcher.metadata_pieces.insert(2, vec![3u8; 50]);
        let result = fetcher.assemble_metadata();
        // Should stop at gap, assembled size != total_size
        assert!(result.is_err());
        match result.unwrap_err() {
            MetadataError::Incomplete => {}
            other => panic!("expected Incomplete, got: {other:?}"),
        }
    }

    #[test]
    fn test_assemble_metadata_size_mismatch() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        fetcher.total_size = Some(200);
        fetcher.metadata_pieces.insert(0, vec![1u8; 50]);
        fetcher.metadata_pieces.insert(1, vec![2u8; 50]);
        // Total received = 100, but total_size = 200
        let result = fetcher.assemble_metadata();
        assert!(result.is_err());
        match result.unwrap_err() {
            MetadataError::Incomplete => {}
            other => panic!("expected Incomplete, got: {other:?}"),
        }
    }

    #[test]
    fn test_assemble_metadata_exact_fit() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        let data = vec![0xABu8; 16384]; // exactly METADATA_PIECE_SIZE
        fetcher.total_size = Some(16384);
        fetcher.metadata_pieces.insert(0, data.clone());
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 16384);
    }

    #[test]
    fn test_assemble_metadata_oversized_piece() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        let data = vec![0u8; 300];
        fetcher.total_size = Some(100);
        fetcher.metadata_pieces.insert(0, data);
        let result = fetcher.assemble_metadata();
        // assembled len is 300 but total_size is 100, so len != total_size → Incomplete
        assert!(result.is_err());
        match result.unwrap_err() {
            MetadataError::Incomplete => {}
            other => panic!("expected Incomplete, got: {other:?}"),
        }
    }

    #[test]
    fn test_assemble_metadata_zero_total_size() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        fetcher.total_size = Some(0);
        // No pieces needed
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_assemble_metadata_many_pieces() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        let piece_size = 100;
        let num_pieces = 50;
        let total = piece_size * num_pieces;
        fetcher.total_size = Some(total);
        for i in 0..num_pieces {
            let data = vec![(i % 256) as u8; piece_size];
            fetcher.metadata_pieces.insert(i, data);
        }
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), total);
    }

    // ── verify_metadata ──

    #[test]
    fn test_verify_metadata_correct_hash() {
        // Create some metadata bytes and compute their SHA-1
        let metadata = b"test metadata content for hashing";
        let hash = Sha1::digest(metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_metadata_wrong_hash() {
        let metadata = b"test metadata content";
        let fetcher = MetadataFetcher::new([0u8; 20]); // wrong hash
        let result = fetcher.verify_metadata(metadata);
        assert!(result.is_err());
        match result.unwrap_err() {
            MetadataError::VerificationFailed => {}
            other => panic!("expected VerificationFailed, got: {other:?}"),
        }
    }

    #[test]
    fn test_verify_metadata_empty_data() {
        let metadata = b"";
        let hash = Sha1::digest(metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_metadata_all_zeros() {
        let metadata = [0u8; 100];
        let hash = Sha1::digest(&metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(&metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_metadata_all_ff() {
        let metadata = [0xFFu8; 100];
        let hash = Sha1::digest(&metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(&metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_metadata_single_byte_mismatch() {
        let metadata = b"test data here";
        let hash = Sha1::digest(metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);
        // Flip one bit in the hash
        info_hash[0] ^= 0x01;

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(metadata);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_metadata_large_data() {
        let metadata = vec![0x42u8; 1024 * 1024]; // 1MB
        let hash = Sha1::digest(&metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(&metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_metadata_unicode_content() {
        let metadata = "你好世界🌍".as_bytes();
        let hash = Sha1::digest(metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        let result = fetcher.verify_metadata(metadata);
        assert!(result.is_ok());
    }

    // ── assemble + verify integration ──

    #[test]
    fn test_assemble_then_verify_success() {
        let metadata_content = b"complete torrent metadata for testing";
        let hash = Sha1::digest(metadata_content);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let mut fetcher = MetadataFetcher::new(info_hash);
        fetcher.total_size = Some(metadata_content.len());
        fetcher.metadata_pieces.insert(0, metadata_content.to_vec());

        let assembled = fetcher.assemble_metadata().unwrap();
        assert!(fetcher.verify_metadata(&assembled).is_ok());
    }

    #[test]
    fn test_assemble_then_verify_failure() {
        let metadata_content = b"some metadata";
        let mut fetcher = MetadataFetcher::new([0u8; 20]); // wrong hash
        fetcher.total_size = Some(metadata_content.len());
        fetcher.metadata_pieces.insert(0, metadata_content.to_vec());

        let assembled = fetcher.assemble_metadata().unwrap();
        assert!(fetcher.verify_metadata(&assembled).is_err());
    }

    #[test]
    fn test_assemble_multi_piece_then_verify() {
        let part1 = b"first half of metadata ";
        let part2 = b"second half of metadata";
        let mut full = Vec::new();
        full.extend_from_slice(part1);
        full.extend_from_slice(part2);

        let hash = Sha1::digest(&full);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let mut fetcher = MetadataFetcher::new(info_hash);
        fetcher.total_size = Some(full.len());
        fetcher.metadata_pieces.insert(0, part1.to_vec());
        fetcher.metadata_pieces.insert(1, part2.to_vec());

        let assembled = fetcher.assemble_metadata().unwrap();
        assert_eq!(assembled, full);
        assert!(fetcher.verify_metadata(&assembled).is_ok());
    }

    // ── Edge cases ──

    #[test]
    fn test_fetcher_with_binary_hash() {
        let info_hash = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13,
        ];
        let fetcher = MetadataFetcher::new(info_hash);
        assert_eq!(fetcher.info_hash, info_hash);
    }

    #[test]
    fn test_assemble_metadata_pieces_out_of_order_insert() {
        // Insert pieces in reverse order, assemble should still work by index
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        fetcher.total_size = Some(150);
        fetcher.metadata_pieces.insert(2, vec![3u8; 50]);
        fetcher.metadata_pieces.insert(0, vec![1u8; 50]);
        fetcher.metadata_pieces.insert(1, vec![2u8; 50]);
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        let assembled = result.unwrap();
        assert_eq!(assembled.len(), 150);
        assert!(assembled[..50].iter().all(|&b| b == 1));
        assert!(assembled[50..100].iter().all(|&b| b == 2));
        assert!(assembled[100..].iter().all(|&b| b == 3));
    }

    #[test]
    fn test_assemble_metadata_single_byte_pieces() {
        let mut fetcher = MetadataFetcher::new([0u8; 20]);
        fetcher.total_size = Some(3);
        fetcher.metadata_pieces.insert(0, vec![0xAA]);
        fetcher.metadata_pieces.insert(1, vec![0xBB]);
        fetcher.metadata_pieces.insert(2, vec![0xCC]);
        let result = fetcher.assemble_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_verify_metadata_deterministic() {
        let metadata = b"deterministic test";
        let hash = Sha1::digest(metadata);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let fetcher = MetadataFetcher::new(info_hash);
        // Verify same data twice
        assert!(fetcher.verify_metadata(metadata).is_ok());
        assert!(fetcher.verify_metadata(metadata).is_ok());
    }

    #[test]
    fn test_multiple_fetchers_independent() {
        let data = b"shared test data";
        let hash = Sha1::digest(data);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        let f1 = MetadataFetcher::new(info_hash);
        let f2 = MetadataFetcher::new(info_hash);
        // Both should verify the same data successfully
        assert!(f1.verify_metadata(data).is_ok());
        assert!(f2.verify_metadata(data).is_ok());
    }
}
