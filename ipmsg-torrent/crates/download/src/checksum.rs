//! Post-download checksum verification
//!
//! Supports MD5, SHA-1, SHA-256, and ED2K (MD4-based) hash verification.
//! Files are streamed from disk to avoid loading entire files into memory.

use std::path::Path;
use tokio::io::{AsyncReadExt, BufReader};

/// Supported checksum algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChecksumAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Ed2k,
}

impl ChecksumAlgorithm {
    /// Parse algorithm name from string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "md5" => Some(Self::Md5),
            "sha1" | "sha-1" => Some(Self::Sha1),
            "sha256" | "sha-256" => Some(Self::Sha256),
            "ed2k" | "ed2k-hash" => Some(Self::Ed2k),
            _ => None,
        }
    }

    /// Expected hash length in hex characters.
    pub fn hex_len(self) -> usize {
        match self {
            Self::Md5 => 32,
            Self::Sha1 => 40,
            Self::Sha256 => 64,
            Self::Ed2k => 32,
        }
    }

    /// Algorithm name as string.
    pub fn name(self) -> &'static str {
        match self {
            Self::Md5 => "MD5",
            Self::Sha1 => "SHA-1",
            Self::Sha256 => "SHA-256",
            Self::Ed2k => "ED2K",
        }
    }
}

/// Errors from checksum verification.
#[derive(Debug, thiserror::Error)]
pub enum ChecksumError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid hex string: {0}")]
    InvalidHex(String),
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    Mismatch { expected: String, actual: String },
    #[error("unknown algorithm: {0}")]
    UnknownAlgorithm(String),
}

/// Result of a checksum verification.
#[derive(Debug, Clone)]
pub struct ChecksumResult {
    pub algorithm: ChecksumAlgorithm,
    pub expected: String,
    pub actual: String,
    pub matched: bool,
}

/// Verify a file against an expected hex-encoded hash.
///
/// Streams the file in chunks to avoid loading it entirely into memory.
pub async fn verify_file(
    path: &Path,
    expected_hex: &str,
    algorithm: ChecksumAlgorithm,
) -> Result<ChecksumResult, ChecksumError> {
    let expected_hex = expected_hex.to_lowercase();
    validate_hex(&expected_hex, algorithm)?;

    let actual_hex = compute_hash(path, algorithm).await?;
    let matched = actual_hex == expected_hex;

    Ok(ChecksumResult {
        algorithm,
        expected: expected_hex,
        actual: actual_hex,
        matched,
    })
}

/// Compute the hash of a file, streaming in chunks.
pub async fn compute_hash(
    path: &Path,
    algorithm: ChecksumAlgorithm,
) -> Result<String, ChecksumError> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buf = vec![0u8; 64 * 1024];

    match algorithm {
        ChecksumAlgorithm::Md5 => {
            use md5::Digest;
            let mut hasher = md5::Md5::new();
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        ChecksumAlgorithm::Sha1 => {
            use sha1::Digest;
            let mut hasher = sha1::Sha1::new();
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        ChecksumAlgorithm::Sha256 => {
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        ChecksumAlgorithm::Ed2k => {
            // ED2K hash: split file into 9.28MB chunks, MD4 each chunk,
            // then MD4 the concatenation of chunk hashes
            use md4::Digest;
            const CHUNK_SIZE: usize = 9_728_000; // 9500 KiB = 9.28 MB

            let mut chunk_hashes = Vec::new();
            let mut chunk_hasher = md4::Md4::new();
            let mut chunk_bytes = 0usize;

            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }

                let mut offset = 0;
                while offset < n {
                    let remaining_in_chunk = CHUNK_SIZE - chunk_bytes;
                    let take = std::cmp::min(n - offset, remaining_in_chunk);
                    chunk_hasher.update(&buf[offset..offset + take]);
                    chunk_bytes += take;
                    offset += take;

                    if chunk_bytes == CHUNK_SIZE {
                        chunk_hashes.push(chunk_hasher.finalize_reset().to_vec());
                        chunk_bytes = 0;
                    }
                }
            }

            // Finalize last chunk
            if chunk_bytes > 0 {
                chunk_hashes.push(chunk_hasher.finalize_reset().to_vec());
            }

            // If file fits in one chunk, that's the hash
            if chunk_hashes.len() == 1 {
                Ok(hex::encode(&chunk_hashes[0]))
            } else {
                // Otherwise, MD4 the concatenation of all chunk hashes
                let mut final_hasher = md4::Md4::new();
                for h in &chunk_hashes {
                    final_hasher.update(h);
                }
                Ok(hex::encode(final_hasher.finalize()))
            }
        }
    }
}

/// Validate a hex string has the correct length for the algorithm.
fn validate_hex(hex_str: &str, algo: ChecksumAlgorithm) -> Result<(), ChecksumError> {
    if hex_str.len() != algo.hex_len() {
        return Err(ChecksumError::InvalidHex(format!(
            "expected {} hex chars for {}, got {}",
            algo.hex_len(),
            algo.name(),
            hex_str.len()
        )));
    }
    if !hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ChecksumError::InvalidHex("non-hex character found".into()));
    }
    Ok(())
}

/// Try to detect the algorithm from a hex hash string by its length.
pub fn detect_algorithm(hex_hash: &str) -> Option<ChecksumAlgorithm> {
    match hex_hash.len() {
        32 => Some(ChecksumAlgorithm::Md5), // Could also be ED2K, default to MD5
        40 => Some(ChecksumAlgorithm::Sha1),
        64 => Some(ChecksumAlgorithm::Sha256),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_md5_verification() {
        // MD5 of empty string = d41d8cd98f00b204e9800998ecf8427e
        let mut f = NamedTempFile::new().unwrap();
        // Write nothing (empty file)
        f.flush().unwrap();

        let result = verify_file(
            f.path(),
            "d41d8cd98f00b204e9800998ecf8427e",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();
        assert!(result.matched, "MD5 of empty file should match");
    }

    #[tokio::test]
    async fn test_sha1_verification() {
        // SHA-1 of empty string = da39a3ee5e6b4b0d3255bfef95601890afd80709
        let f = NamedTempFile::new().unwrap();

        let result = verify_file(
            f.path(),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            ChecksumAlgorithm::Sha1,
        )
        .await
        .unwrap();
        assert!(result.matched, "SHA-1 of empty file should match");
    }

    #[tokio::test]
    async fn test_sha256_verification() {
        // SHA-256 of empty string = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let f = NamedTempFile::new().unwrap();

        let result = verify_file(
            f.path(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ChecksumAlgorithm::Sha256,
        )
        .await
        .unwrap();
        assert!(result.matched, "SHA-256 of empty file should match");
    }

    #[tokio::test]
    async fn test_checksum_mismatch() {
        let f = NamedTempFile::new().unwrap();

        let result = verify_file(
            f.path(),
            "00000000000000000000000000000000",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();
        assert!(!result.matched, "Should not match wrong hash");
        assert_ne!(result.expected, result.actual);
    }

    #[tokio::test]
    async fn test_invalid_hex_length() {
        let f = NamedTempFile::new().unwrap();

        let err = verify_file(f.path(), "abcd", ChecksumAlgorithm::Md5)
            .await
            .unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[tokio::test]
    async fn test_invalid_hex_chars() {
        let f = NamedTempFile::new().unwrap();

        let err = verify_file(
            f.path(),
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[tokio::test]
    async fn test_file_with_content() {
        use std::io::Write;
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hello world").unwrap();
        f.flush().unwrap();

        // MD5 of "hello world" = 5eb63bbbe01eeed093cb22bb8f5acdc3
        let result = verify_file(
            f.path(),
            "5eb63bbbe01eeed093cb22bb8f5acdc3",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();
        assert!(result.matched);
    }

    #[test]
    fn test_parse_algorithm() {
        assert_eq!(
            ChecksumAlgorithm::parse("md5"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("sha-256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("ed2k"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(ChecksumAlgorithm::parse("unknown"), None);
    }

    #[test]
    fn test_detect_algorithm() {
        assert_eq!(
            detect_algorithm("d41d8cd98f00b204e9800998ecf8427e"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            detect_algorithm("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            detect_algorithm("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(detect_algorithm("tooshort"), None);
    }

    #[test]
    fn test_algorithm_hex_len() {
        assert_eq!(ChecksumAlgorithm::Md5.hex_len(), 32);
        assert_eq!(ChecksumAlgorithm::Sha1.hex_len(), 40);
        assert_eq!(ChecksumAlgorithm::Sha256.hex_len(), 64);
        assert_eq!(ChecksumAlgorithm::Ed2k.hex_len(), 32);
    }

    #[tokio::test]
    async fn test_ed2k_empty_file() {
        // ED2K hash of empty file = 31d6cfe0d16ae931b73c59d7e0c089c0 (MD4 of empty)
        let f = NamedTempFile::new().unwrap();

        let result = verify_file(
            f.path(),
            "31d6cfe0d16ae931b73c59d7e0c089c0",
            ChecksumAlgorithm::Ed2k,
        )
        .await
        .unwrap();
        assert!(result.matched, "ED2K of empty file should match");
    }

    #[tokio::test]
    async fn test_compute_hash_returns_correct_length() {
        let f = NamedTempFile::new().unwrap();

        let md5 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert_eq!(md5.len(), 32);

        let sha1 = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        assert_eq!(sha1.len(), 40);

        let sha256 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(sha256.len(), 64);
    }
}
