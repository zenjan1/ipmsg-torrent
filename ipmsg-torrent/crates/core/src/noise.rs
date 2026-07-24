use snow::Builder;
use std::collections::HashMap;
use thiserror::Error;

/// Noise session state for E2E encryption between peers
/// Inspired by bitchat's Noise_XX_25519_ChaChaPoly_SHA256 implementation
pub struct NoiseSession {
    local_key: [u8; 32],
    state: SessionState,
    rekey_count: u64,
}

enum SessionState {
    Handshake(snow::HandshakeState),
    Transport(snow::TransportState),
    Empty,
}

/// Maximum noise message size
const MAX_NOISE_MSG: usize = 65535;

#[derive(Debug, Error)]
pub enum NoiseError {
    #[error("handshake not complete")]
    HandshakeIncomplete,
    #[error("invalid public key")]
    InvalidPublicKey,
    #[error("encryption failed")]
    EncryptionFailed,
    #[error("decryption failed")]
    DecryptionFailed,
    #[error("noise error: {0}")]
    Noise(String),
}

fn generate_key() -> [u8; 32] {
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    key
}

impl NoiseSession {
    pub fn new_initiator() -> Result<Self, NoiseError> {
        let params: snow::params::NoiseParams = "Noise_XX_25519_ChaChaPoly_SHA256"
            .parse()
            .map_err(|e: snow::Error| NoiseError::Noise(e.to_string()))?;
        let key = generate_key();
        let handshake = Builder::new(params)
            .local_private_key(&key)
            .build_initiator()
            .map_err(|e: snow::Error| NoiseError::Noise(e.to_string()))?;
        Ok(Self {
            local_key: key,
            state: SessionState::Handshake(handshake),
            rekey_count: 0,
        })
    }

    pub fn new_responder() -> Result<Self, NoiseError> {
        let params: snow::params::NoiseParams = "Noise_XX_25519_ChaChaPoly_SHA256"
            .parse()
            .map_err(|e: snow::Error| NoiseError::Noise(e.to_string()))?;
        let key = generate_key();
        let handshake = Builder::new(params)
            .local_private_key(&key)
            .build_responder()
            .map_err(|e: snow::Error| NoiseError::Noise(e.to_string()))?;
        Ok(Self {
            local_key: key,
            state: SessionState::Handshake(handshake),
            rekey_count: 0,
        })
    }

    /// Write the next handshake message. Returns the message bytes and whether handshake is done.
    pub fn write_handshake_message(&mut self) -> Result<(Vec<u8>, bool), NoiseError> {
        match &mut self.state {
            SessionState::Handshake(hs) => {
                let mut buf = vec![0u8; MAX_NOISE_MSG];
                let len = hs
                    .write_message(&[], &mut buf)
                    .map_err(|e: snow::Error| NoiseError::Noise(e.to_string()))?;
                buf.truncate(len);
                let done = hs.is_handshake_finished();
                if done {
                    self.finish_handshake();
                }
                Ok((buf, done))
            }
            _ => Err(NoiseError::HandshakeIncomplete),
        }
    }

    /// Read an incoming handshake message. Returns whether handshake is done.
    pub fn read_handshake_message(&mut self, buf: &[u8]) -> Result<bool, NoiseError> {
        match &mut self.state {
            SessionState::Handshake(hs) => {
                let mut out = vec![0u8; MAX_NOISE_MSG];
                hs.read_message(buf, &mut out)
                    .map_err(|e: snow::Error| NoiseError::Noise(e.to_string()))?;
                let done = hs.is_handshake_finished();
                if done {
                    self.finish_handshake();
                }
                Ok(done)
            }
            _ => Err(NoiseError::HandshakeIncomplete),
        }
    }

    fn finish_handshake(&mut self) {
        let old_state = std::mem::replace(&mut self.state, SessionState::Empty);
        if let SessionState::Handshake(hs) = old_state {
            if let Ok(ts) = hs.into_transport_mode() {
                self.state = SessionState::Transport(ts);
                tracing::info!("Noise handshake completed");
            } else {
                self.state = SessionState::Empty;
            }
        } else {
            self.state = old_state;
        }
    }

    /// Encrypt plaintext to ciphertext
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        match &mut self.state {
            SessionState::Transport(ts) => {
                let max_len = plaintext.len() + 16; // +16 for Poly1305 MAC
                let mut buf = vec![0u8; max_len];
                let len = ts
                    .write_message(plaintext, &mut buf)
                    .map_err(|_| NoiseError::EncryptionFailed)?;
                buf.truncate(len);
                Ok(buf)
            }
            _ => Err(NoiseError::HandshakeIncomplete),
        }
    }

    /// Decrypt ciphertext to plaintext
    pub fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        match &mut self.state {
            SessionState::Transport(ts) => {
                let mut buf = vec![0u8; ciphertext.len()];
                let len = ts
                    .read_message(ciphertext, &mut buf)
                    .map_err(|_| NoiseError::DecryptionFailed)?;
                buf.truncate(len);
                Ok(buf)
            }
            _ => Err(NoiseError::HandshakeIncomplete),
        }
    }

    pub fn rekey(&mut self) {
        self.rekey_count += 1;
        tracing::debug!(
            rekey_count = self.rekey_count,
            "Noise session re-key requested"
        );
    }

    pub fn is_ready(&self) -> bool {
        matches!(&self.state, SessionState::Transport(_))
    }

    #[allow(dead_code)]
    pub fn local_public_key(&self) -> [u8; 32] {
        use x25519_dalek::x25519;
        let basepoint: [u8; 32] = [9u8; 32];
        x25519(self.local_key, basepoint)
    }
}

pub struct NoiseSessionManager {
    sessions: HashMap<String, NoiseSession>,
    rekey_threshold: u64,
    message_counts: HashMap<String, u64>,
}

impl NoiseSessionManager {
    pub fn new(rekey_threshold: u64) -> Self {
        Self {
            sessions: HashMap::new(),
            rekey_threshold,
            message_counts: HashMap::new(),
        }
    }

    pub fn get_or_create_initiator(
        &mut self,
        peer_id: &str,
    ) -> Result<&mut NoiseSession, NoiseError> {
        use std::collections::hash_map::Entry;
        match self.sessions.entry(peer_id.to_string()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let session = NoiseSession::new_initiator()?;
                Ok(entry.insert(session))
            }
        }
    }

    pub fn insert_responder(&mut self, peer_id: &str) -> Result<&mut NoiseSession, NoiseError> {
        use std::collections::hash_map::Entry;
        match self.sessions.entry(peer_id.to_string()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let session = NoiseSession::new_responder()?;
                Ok(entry.insert(session))
            }
        }
    }

    pub fn get_mut(&mut self, peer_id: &str) -> Option<&mut NoiseSession> {
        self.sessions.get_mut(peer_id)
    }

    pub fn on_message_sent(&mut self, peer_id: &str) {
        let count = self.message_counts.entry(peer_id.to_string()).or_insert(0);
        *count += 1;
        if *count >= self.rekey_threshold {
            *count = 0;
            if let Some(session) = self.sessions.get_mut(peer_id) {
                session.rekey();
            }
        }
    }

    pub fn remove(&mut self, peer_id: &str) {
        self.sessions.remove(peer_id);
        self.message_counts.remove(peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_roundtrip() {
        let mut initiator = NoiseSession::new_initiator().unwrap();
        let mut responder = NoiseSession::new_responder().unwrap();

        // XX pattern 3-message handshake:
        // msg1: I -> R (e)
        // msg2: R -> I (e, s, sig)
        // msg3: I -> R (s, sig)
        let (msg1, _) = initiator.write_handshake_message().unwrap();
        let _ = responder.read_handshake_message(&msg1).unwrap();
        let (msg2, _) = responder.write_handshake_message().unwrap();
        let _ = initiator.read_handshake_message(&msg2).unwrap();
        let (msg3, _) = initiator.write_handshake_message().unwrap();
        let _ = responder.read_handshake_message(&msg3).unwrap();

        // Both sides should be in transport mode after full exchange
        assert!(initiator.is_ready());
        assert!(responder.is_ready());

        // Verify bidirectional encryption works
        let plaintext = b"test message from initiator";
        let ciphertext = initiator.encrypt(plaintext).unwrap();
        let decrypted = responder.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);

        let plaintext2 = b"test message from responder";
        let ciphertext2 = responder.encrypt(plaintext2).unwrap();
        let decrypted2 = initiator.decrypt(&ciphertext2).unwrap();
        assert_eq!(decrypted2, plaintext2);
    }

    #[test]
    fn test_encrypt_decrypt() {
        let mut initiator = NoiseSession::new_initiator().unwrap();
        let mut responder = NoiseSession::new_responder().unwrap();

        let (msg1, _) = initiator.write_handshake_message().unwrap();
        let _ = responder.read_handshake_message(&msg1).unwrap();
        let (msg2, _) = responder.write_handshake_message().unwrap();
        let _ = initiator.read_handshake_message(&msg2).unwrap();
        let (msg3, _) = initiator.write_handshake_message().unwrap();
        let _ = responder.read_handshake_message(&msg3).unwrap();

        let plaintext = b"hello world";
        let ciphertext = initiator.encrypt(plaintext).unwrap();
        let decrypted = responder.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }
}
