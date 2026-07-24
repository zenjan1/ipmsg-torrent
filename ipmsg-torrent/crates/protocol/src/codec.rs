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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ChannelId, ChatMessage, FileRef, FileTransferMsg, MessageType};

    #[test]
    fn test_encode_decode_text_message_roundtrip() {
        let msg = ChatMessage::new_text(
            "peer_a".to_string(),
            Some("peer_b".to_string()),
            "hello world".to_string(),
        );
        let encoded = encode_message(&msg);
        assert!(!encoded.is_empty());

        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.id, msg.id);
        assert_eq!(decoded.from, "peer_a");
        assert_eq!(decoded.to, Some("peer_b".to_string()));
        assert_eq!(decoded.text_content(), Some("hello world"));
    }

    #[test]
    fn test_encode_decode_presence_message() {
        let msg = ChatMessage::new_presence(
            "peer_x".to_string(),
            "alice".to_string(),
            vec!["rust".to_string(), "linux".to_string()],
        );
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        match &decoded.kind {
            MessageType::Presence {
                username,
                platforms,
                ..
            } => {
                assert_eq!(username, "alice");
                assert_eq!(platforms, &vec!["rust".to_string(), "linux".to_string()]);
            }
            _ => panic!("expected Presence message"),
        }
    }

    #[test]
    fn test_encode_decode_channel_message() {
        let msg = ChatMessage::for_channel(
            "peer_c".to_string(),
            ChannelId::Group("devs".to_string()),
            "channel message".to_string(),
        );
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert!(decoded.channel.is_some());
        assert_eq!(
            decoded.channel.unwrap(),
            ChannelId::Group("devs".to_string())
        );
    }

    #[test]
    fn test_encode_decode_ack_message() {
        let msg = ChatMessage::new_ack(
            "peer_a".to_string(),
            "peer_b".to_string(),
            vec!["msg-1".to_string(), "msg-2".to_string()],
        );
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        match &decoded.kind {
            MessageType::Ack { message_ids } => {
                assert_eq!(message_ids.len(), 2);
                assert_eq!(message_ids[0], "msg-1");
                assert_eq!(message_ids[1], "msg-2");
            }
            _ => panic!("expected Ack message"),
        }
    }

    #[test]
    fn test_encode_decode_typing_message() {
        let msg = ChatMessage::new_typing("peer_a".to_string(), "peer_b".to_string());
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.kind.label(), "typing");
        assert_eq!(decoded.ttl, 5);
    }

    #[test]
    fn test_encode_decode_file_transfer_msg() {
        let file_ref = FileRef::new(
            "test.txt".to_string(),
            1024,
            "text/plain".to_string(),
            b"test data",
        );
        let msg = FileTransferMsg::Offer { file_ref };
        let encoded = encode_file_msg(&msg);
        assert!(!encoded.is_empty());

        let decoded = decode_file_msg(&encoded).unwrap();
        match decoded {
            FileTransferMsg::Offer { file_ref } => {
                assert_eq!(file_ref.name, "test.txt");
                assert_eq!(file_ref.size, 1024);
                assert_eq!(file_ref.mime_type, "text/plain");
            }
            _ => panic!("expected Offer"),
        }
    }

    #[test]
    fn test_decode_invalid_bytes_returns_error() {
        let bad_bytes = vec![0xFF, 0xFE, 0xFD];
        let result = decode_message(&bad_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_message_with_sequence_and_reply() {
        let msg = ChatMessage::new_text(
            "peer_a".to_string(),
            Some("peer_b".to_string()),
            "reply text".to_string(),
        )
        .with_sequence(42)
        .with_reply("original-msg-id".to_string());

        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.reply_to, Some("original-msg-id".to_string()));
    }

    #[test]
    fn test_message_with_ttl() {
        let msg =
            ChatMessage::new_text("peer_a".to_string(), None, "ephemeral".to_string()).with_ttl(10);

        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.ttl, 10);
        assert!(!decoded.is_expired());
    }

    #[test]
    fn test_file_ref_new_computes_hash() {
        let data = b"hello file content";
        let file_ref = FileRef::new(
            "doc.txt".to_string(),
            data.len() as u64,
            "text/plain".to_string(),
            data,
        );
        assert!(!file_ref.hash.is_empty());
        assert_eq!(file_ref.chunks, 1); // small file = 1 chunk
        assert_eq!(file_ref.chunk_size, 256 * 1024);
    }

    #[test]
    fn test_file_ref_chunk_count() {
        // 1MB file should need 4 chunks at 256KB each
        let data = vec![0u8; 1024 * 1024];
        let file_ref = FileRef::new(
            "big.bin".to_string(),
            data.len() as u64,
            "application/octet-stream".to_string(),
            &data,
        );
        assert_eq!(file_ref.chunks, 4);
    }
}
