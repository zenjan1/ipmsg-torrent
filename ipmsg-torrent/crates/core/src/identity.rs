use ed25519_dalek::{SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH};
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
        let public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(
            &verifying_key.to_bytes(),
        )
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
        let public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(
            &verifying_key.to_bytes(),
        )
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
        self.signing_key
            .verifying_key()
            .verify(data, &sig)
            .is_ok()
    }

    /// Get the verifying (public) key for sharing
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Convert to libp2p Keypair for use in Swarm
    pub fn to_keypair(&self) -> libp2p::identity::Keypair {
        let mut bytes = self.signing_key.to_bytes();
        let keypair = libp2p::identity::ed25519::Keypair::try_from_bytes(&mut bytes)
            .expect("valid keypair");
        keypair.into()
    }
}
