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

    #[test]
    fn test_metadata_fetcher_creation() {
        let info_hash = [0u8; 20];
        let fetcher = MetadataFetcher::new(info_hash);
        assert_eq!(fetcher.info_hash, info_hash);
        assert!(fetcher.metadata_pieces.is_empty());
        assert!(fetcher.total_size.is_none());
    }
}
