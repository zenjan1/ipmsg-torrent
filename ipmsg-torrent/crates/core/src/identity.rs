use ed25519_dalek::{SECRET_KEY_LENGTH, SigningKey, Verifier, VerifyingKey};
use libp2p::PeerId;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("key error: {0}")]
    Key(String),
}

/// Cryptographic identity for this node
pub struct Identity {
    signing_key: SigningKey,
    peer_id: PeerId,
}

impl Identity {
    /// Generate a new random identity
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let verifying_key = signing_key.verifying_key();
        let public_key =
            libp2p::identity::ed25519::PublicKey::try_from_bytes(&verifying_key.to_bytes())
                .expect("valid ed25519 key");
        let peer_id = PeerId::from_public_key(&public_key.into());

        Self {
            signing_key,
            peer_id,
        }
    }

    /// Load identity from file, or create a new one if it doesn't exist
    pub fn load_or_create(path: &Path) -> Result<Self, IdentityError> {
        if path.exists() {
            Self::load(path)
        } else {
            let identity = Self::generate();
            identity.save(path)?;
            Ok(identity)
        }
    }

    /// Load identity from a key file
    fn load(path: &Path) -> Result<Self, IdentityError> {
        let bytes = std::fs::read(path)?;
        let key_bytes: [u8; SECRET_KEY_LENGTH] = bytes
            .try_into()
            .map_err(|_| IdentityError::Key("invalid key length".to_string()))?;

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        let public_key =
            libp2p::identity::ed25519::PublicKey::try_from_bytes(&verifying_key.to_bytes())
                .map_err(|e| IdentityError::Key(e.to_string()))?;
        let peer_id = PeerId::from_public_key(&public_key.into());

        Ok(Self {
            signing_key,
            peer_id,
        })
    }

    /// Save identity to file
    fn save(&self, path: &Path) -> Result<(), IdentityError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, self.signing_key.to_bytes())?;
        Ok(())
    }

    /// Get the PeerID as a libp2p PeerId
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Get the PeerID as a base58 string
    pub fn peer_id_str(&self) -> String {
        self.peer_id.to_base58()
    }

    /// Sign arbitrary data
    pub fn sign(&self, data: &[u8]) -> Vec<u8> {
        use ed25519_dalek::Signer;
        self.signing_key.sign(data).to_bytes().to_vec()
    }

    /// Verify a signature against data using our public key
    pub fn verify(&self, data: &[u8], signature: &[u8]) -> bool {
        use ed25519_dalek::Signature;
        let Ok(sig) = Signature::from_slice(signature) else {
            return false;
        };
        self.signing_key.verifying_key().verify(data, &sig).is_ok()
    }

    /// Get the verifying (public) key for sharing
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Convert to libp2p Keypair for use in Swarm
    pub fn to_keypair(&self) -> libp2p::identity::Keypair {
        // libp2p expects 64 bytes: 32 secret + 32 public
        let mut bytes = [0u8; 64];
        let secret = self.signing_key.to_bytes();
        let public = self.signing_key.verifying_key().to_bytes();
        bytes[..32].copy_from_slice(&secret);
        bytes[32..].copy_from_slice(&public);
        let keypair =
            libp2p::identity::ed25519::Keypair::try_from_bytes(&mut bytes).expect("valid keypair");
        keypair.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_creates_valid_identity() {
        let id = Identity::generate();
        assert!(!id.peer_id_str().is_empty());
        // PeerID should be valid base58
        assert!(id.peer_id_str().chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_sign_and_verify() {
        let id = Identity::generate();
        let data = b"test message to sign";
        let sig = id.sign(data);
        assert!(!sig.is_empty());
        assert!(id.verify(data, &sig));
    }

    #[test]
    fn test_verify_wrong_data_fails() {
        let id = Identity::generate();
        let sig = id.sign(b"correct data");
        assert!(!id.verify(b"wrong data", &sig));
    }

    #[test]
    fn test_verify_wrong_signature_fails() {
        let id = Identity::generate();
        let data = b"some data";
        assert!(!id.verify(data, &[0u8; 64]));
    }

    #[test]
    fn test_load_or_create_creates_new() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("test.key");
        assert!(!key_path.exists());

        let id1 = Identity::load_or_create(&key_path).unwrap();
        assert!(key_path.exists());

        // Loading again should give same identity
        let id2 = Identity::load_or_create(&key_path).unwrap();
        assert_eq!(id1.peer_id_str(), id2.peer_id_str());
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("identity.key");

        let id = Identity::generate();
        id.save(&key_path).unwrap();

        let loaded = Identity::load(&key_path).unwrap();
        assert_eq!(id.peer_id_str(), loaded.peer_id_str());

        // Verify signature with loaded key
        let data = b"cross-session data";
        let sig = id.sign(data);
        assert!(loaded.verify(data, &sig));
    }

    #[test]
    fn test_to_keypair_produces_valid_libp2p_key() {
        let id = Identity::generate();
        let keypair = id.to_keypair();
        let peer_id = libp2p::PeerId::from_public_key(&keypair.public());
        assert_eq!(peer_id.to_base58(), id.peer_id_str());
    }

    #[test]
    fn test_verifying_key_matches_signing_key() {
        let id = Identity::generate();
        let vk = id.verifying_key();
        let data = b"test";
        let sig = id.sign(data);
        use ed25519_dalek::Signature;
        use ed25519_dalek::Verifier;
        let sig_obj = Signature::from_slice(&sig).unwrap();
        assert!(vk.verify(data, &sig_obj).is_ok());
    }
}
