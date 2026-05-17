use crate::message::{ChatMessage, FileTransferMsg};

/// Serialize a ChatMessage to CBOR bytes
pub fn encode_message(msg: &ChatMessage) -> Vec<u8> {
    serde_cbor::to_vec(msg).unwrap_or_default()
}

/// Deserialize CBOR bytes to a ChatMessage
pub fn decode_message(bytes: &[u8]) -> Result<ChatMessage, String> {
    serde_cbor::from_slice(bytes).map_err(|e| e.to_string())
}

/// Serialize a FileTransferMsg to CBOR bytes
pub fn encode_file_msg(msg: &FileTransferMsg) -> Vec<u8> {
    serde_cbor::to_vec(msg).unwrap_or_default()
}

/// Deserialize CBOR bytes to a FileTransferMsg
pub fn decode_file_msg(bytes: &[u8]) -> Result<FileTransferMsg, String> {
    serde_cbor::from_slice(bytes).map_err(|e| e.to_string())
}
