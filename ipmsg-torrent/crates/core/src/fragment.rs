use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default MTU for transport (conservative for BLE compatibility)
const DEFAULT_MTU: usize = 1024;
/// Maximum fragment size after header overhead
const MAX_FRAGMENT_SIZE: usize = DEFAULT_MTU - 64;

#[derive(Debug, Error)]
pub enum FragmentError {
    #[error("fragment already received")]
    DuplicateFragment,
    #[error("invalid fragment index")]
    InvalidFragmentIndex,
    #[error("fragment assembly incomplete")]
    Incomplete,
    #[error("message too large (max 10MB)")]
    MessageTooLarge,
}

/// Fragmented message protocol
/// Inspired by bitchat's fragmentation for large messages over BLE MTU limits
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FragmentMsg {
    /// Start of fragmented message, contains metadata
    Start {
        /// Unique message ID for the complete message
        message_id: String,
        /// Total number of fragments
        total_fragments: u16,
        /// Total size of the original payload in bytes
        total_size: usize,
        /// Content type (e.g., "text", "image", "file")
        content_type: String,
    },
    /// Fragment data chunk
    Data {
        /// Message ID this fragment belongs to
        message_id: String,
        /// Fragment index (0-based)
        index: u16,
        /// Fragment payload data
        data: Vec<u8>,
    },
    /// End of fragmented message (final fragment marker)
    End {
        /// Message ID
        message_id: String,
        /// Final fragment index
        final_index: u16,
    },
}

/// Tracks assembly of a fragmented message
struct FragmentAssembly {
    total_fragments: u16,
    total_size: usize,
    #[allow(dead_code)]
    content_type: String,
    fragments: Vec<Option<Vec<u8>>>,
    received_count: u16,
}

impl FragmentAssembly {
    fn new(total: u16, size: usize, content_type: String) -> Self {
        Self {
            total_fragments: total,
            total_size: size,
            content_type,
            fragments: vec![None; total as usize],
            received_count: 0,
        }
    }

    fn add_fragment(&mut self, index: u16, data: Vec<u8>) -> Result<(), FragmentError> {
        if index >= self.total_fragments {
            return Err(FragmentError::InvalidFragmentIndex);
        }
        if self.fragments[index as usize].is_some() {
            return Err(FragmentError::DuplicateFragment);
        }
        self.fragments[index as usize] = Some(data);
        self.received_count += 1;
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.received_count == self.total_fragments
    }

    fn reassemble(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.total_size);
        for fragment in self.fragments {
            if let Some(data) = fragment {
                result.extend_from_slice(&data);
            }
        }
        result
    }
}

/// Fragment manager handles splitting and reassembling fragmented messages
pub struct FragmentManager {
    /// Active assemblies keyed by message_id
    assemblies: std::collections::HashMap<String, FragmentAssembly>,
    /// Maximum message size (10MB)
    max_message_size: usize,
}

impl FragmentManager {
    pub fn new() -> Self {
        Self {
            assemblies: std::collections::HashMap::new(),
            max_message_size: 10 * 1024 * 1024, // 10MB
        }
    }

    /// Fragment a large payload into a sequence of FragmentMsg
    pub fn fragment(&self, message_id: &str, payload: &[u8], content_type: &str) -> Vec<FragmentMsg> {
        if payload.len() <= MAX_FRAGMENT_SIZE {
            // No fragmentation needed
            return vec![];
        }

        let total_fragments = ((payload.len() as f64) / (MAX_FRAGMENT_SIZE as f64)).ceil() as u16;

        let mut msgs = Vec::with_capacity(total_fragments as usize + 2);

        // Start message
        msgs.push(FragmentMsg::Start {
            message_id: message_id.to_string(),
            total_fragments,
            total_size: payload.len(),
            content_type: content_type.to_string(),
        });

        // Data fragments
        for (i, chunk) in payload.chunks(MAX_FRAGMENT_SIZE).enumerate() {
            msgs.push(FragmentMsg::Data {
                message_id: message_id.to_string(),
                index: i as u16,
                data: chunk.to_vec(),
            });
        }

        // End message
        msgs.push(FragmentMsg::End {
            message_id: message_id.to_string(),
            final_index: total_fragments - 1,
        });

        msgs
    }

    /// Process an incoming fragment and return the complete payload if assembly is done
    pub fn process_fragment(&mut self, msg: FragmentMsg) -> Result<Option<Vec<u8>>, FragmentError> {
        match msg {
            FragmentMsg::Start {
                message_id,
                total_fragments,
                total_size,
                content_type,
            } => {
                if total_size > self.max_message_size {
                    return Err(FragmentError::MessageTooLarge);
                }
                self.assemblies.insert(
                    message_id,
                    FragmentAssembly::new(total_fragments, total_size, content_type),
                );
                Ok(None)
            }
            FragmentMsg::Data {
                message_id,
                index,
                data,
            } => {
                let assembly = self.assemblies.get_mut(&message_id);
                if let Some(asm) = assembly {
                    asm.add_fragment(index, data)?;
                    if asm.is_complete() {
                        let asm = self.assemblies.remove(&message_id).unwrap();
                        Ok(Some(asm.reassemble()))
                    } else {
                        Ok(None)
                    }
                } else {
                    // Received data before start → incomplete
                    Err(FragmentError::Incomplete)
                }
            }
            FragmentMsg::End {
                message_id,
                final_index: _,
            } => {
                // Check if assembly is already complete
                if let Some(asm) = self.assemblies.get(&message_id) {
                    if asm.is_complete() {
                        let asm = self.assemblies.remove(&message_id).unwrap();
                        return Ok(Some(asm.reassemble()));
                    }
                }
                // Otherwise wait for missing fragments
                Ok(None)
            }
        }
    }

    /// Check if a message needs fragmentation
    pub fn needs_fragment(&self, payload_size: usize) -> bool {
        payload_size > MAX_FRAGMENT_SIZE
    }

    /// Clean up stale assemblies (older than timeout)
    pub fn cleanup_stale_assemblies(&mut self) {
        // Simple cleanup: remove assemblies with no recent activity
        // In production, track timestamps per assembly
        self.assemblies.retain(|_, asm| asm.received_count > 0);
    }
}

/// LZ4 compression wrapper for message payloads
/// Inspired by bitchat's LZ4 message compression
pub mod compression {
    /// Compress a payload using LZ4
    pub fn compress(data: &[u8]) -> Vec<u8> {
        lz4_flex::compress_prepend_size(data)
    }

    /// Decompress a payload using LZ4
    pub fn decompress(data: &[u8]) -> Option<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data).ok()
    }

    /// Compress only if it results in smaller output
    pub fn compress_if_smaller(data: &[u8]) -> (Vec<u8>, bool) {
        let compressed = compress(data);
        if compressed.len() < data.len() {
            (compressed, true)
        } else {
            (data.to_vec(), false)
        }
    }
}

/// Message padding to standard block sizes for traffic analysis resistance
/// Inspired by bitchat's fixed-size padding (256, 512, 1024, 2048 bytes)
pub mod padding {
    const BLOCK_SIZES: [usize; 4] = [256, 512, 1024, 2048];

    /// Pad data to the next standard block size
    pub fn pad_to_block(data: &[u8]) -> Vec<u8> {
        let target = BLOCK_SIZES
            .iter()
            .find(|&&size| size >= data.len())
            .copied()
            .unwrap_or(2048);

        let mut result = data.to_vec();
        let pad_len = target - result.len();
        // PKCS#7-style padding
        result.extend(std::iter::repeat(pad_len as u8).take(pad_len));
        result
    }

    /// Remove padding from data
    pub fn unpad(data: &[u8]) -> Option<Vec<u8>> {
        if data.is_empty() {
            return None;
        }
        let pad_len = *data.last()? as usize;
        if pad_len == 0 || pad_len > data.len() {
            return None;
        }
        // Verify padding
        for &b in &data[data.len() - pad_len..] {
            if b as usize != pad_len {
                return None;
            }
        }
        Some(data[..data.len() - pad_len].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fragment_and_reassemble() {
        let manager = FragmentManager::new();

        // Create a payload larger than MTU
        let payload = vec![0xAB; 3000];
        let fragments = manager.fragment("msg-1", &payload, "test");

        assert!(fragments.len() > 2); // Start + data + end

        // Process fragments
        let mut reassembled = None;
        let mut mgr = FragmentManager::new();
        for frag in fragments {
            if let Some(data) = mgr.process_fragment(frag).unwrap() {
                reassembled = Some(data);
            }
        }

        assert!(reassembled.is_some());
        assert_eq!(reassembled.unwrap(), payload);
    }

    #[test]
    fn test_no_fragmentation_small_payload() {
        let manager = FragmentManager::new();
        let payload = vec![0x01; 100]; // Small payload
        let fragments = manager.fragment("msg-2", &payload, "test");
        assert!(fragments.is_empty());
    }

    #[test]
    fn test_padding_roundtrip() {
        use padding::*;

        let data = vec![0x42; 200];
        let padded = pad_to_block(&data);
        assert!(padded.len() >= 256);

        let unpadded = unpad(&padded).unwrap();
        assert_eq!(unpadded, data);
    }

    #[test]
    fn test_compression() {
        use compression::*;

        // Compressible data
        let data = vec![0x00; 1000];
        let (compressed, was_compressed) = compress_if_smaller(&data);
        assert!(was_compressed);
        assert!(compressed.len() < data.len());

        // Already random data (not compressible)
        let random_data: Vec<u8> = (0..100).map(|i| (i % 251) as u8).collect();
        let (result, was_compressed) = compress_if_smaller(&random_data);
        assert!(!was_compressed);
        assert_eq!(result.len(), random_data.len());
    }
}
