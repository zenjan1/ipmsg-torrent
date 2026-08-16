//! Post-download checksum verification
//!
//! Supports MD5, SHA-1, SHA-256, and ED2K (MD4-based) hash verification.
//! Files are streamed from disk to avoid loading entire files into memory.

use std::path::Path;
use tokio::io::{AsyncReadExt, BufReader};

/// Supported checksum algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

    // ─── Additional comprehensive tests ───

    #[test]
    fn test_algorithm_name() {
        assert_eq!(ChecksumAlgorithm::Md5.name(), "MD5");
        assert_eq!(ChecksumAlgorithm::Sha1.name(), "SHA-1");
        assert_eq!(ChecksumAlgorithm::Sha256.name(), "SHA-256");
        assert_eq!(ChecksumAlgorithm::Ed2k.name(), "ED2K");
    }

    #[test]
    fn test_algorithm_clone_copy() {
        let algo = ChecksumAlgorithm::Sha256;
        let cloned = algo;
        assert_eq!(algo, cloned);
    }

    #[test]
    fn test_algorithm_debug() {
        let debug_str = format!("{:?}", ChecksumAlgorithm::Md5);
        assert_eq!(debug_str, "Md5");
    }

    #[test]
    fn test_algorithm_serde_roundtrip() {
        for algo in [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Ed2k,
        ] {
            let json = serde_json::to_string(&algo).unwrap();
            let deserialized: ChecksumAlgorithm = serde_json::from_str(&json).unwrap();
            assert_eq!(algo, deserialized);
        }
    }

    #[test]
    fn test_parse_algorithm_case_insensitive() {
        assert_eq!(
            ChecksumAlgorithm::parse("MD5"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Md5"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA-1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Sha-256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("ED2K"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("ed2k-hash"),
            Some(ChecksumAlgorithm::Ed2k)
        );
    }

    #[test]
    fn test_parse_algorithm_invalid() {
        assert_eq!(ChecksumAlgorithm::parse(""), None);
        assert_eq!(ChecksumAlgorithm::parse("sha512"), None);
        assert_eq!(ChecksumAlgorithm::parse("crc32"), None);
        assert_eq!(ChecksumAlgorithm::parse("blake2"), None);
    }

    #[test]
    fn test_detect_algorithm_edge_cases() {
        // 32 chars could be MD5 or ED2K, defaults to MD5
        assert_eq!(
            detect_algorithm("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(ChecksumAlgorithm::Md5)
        );
        // 40 chars = SHA-1
        assert_eq!(
            detect_algorithm("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(ChecksumAlgorithm::Sha1)
        );
        // 64 chars = SHA-256
        assert_eq!(
            detect_algorithm(&"a".repeat(64)),
            Some(ChecksumAlgorithm::Sha256)
        );
        // Other lengths = None
        assert_eq!(detect_algorithm("abc"), None);
        assert_eq!(detect_algorithm(&"a".repeat(31)), None);
        assert_eq!(detect_algorithm(&"a".repeat(33)), None);
        assert_eq!(detect_algorithm(&"a".repeat(39)), None);
        assert_eq!(detect_algorithm(&"a".repeat(41)), None);
        assert_eq!(detect_algorithm(&"a".repeat(63)), None);
        assert_eq!(detect_algorithm(&"a".repeat(65)), None);
        assert_eq!(detect_algorithm(""), None);
    }

    #[test]
    fn test_validate_hex_valid() {
        // Valid MD5 hex (32 chars)
        assert!(validate_hex("d41d8cd98f00b204e9800998ecf8427e", ChecksumAlgorithm::Md5).is_ok());
        // Valid SHA-1 hex (40 chars)
        assert!(
            validate_hex(
                "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                ChecksumAlgorithm::Sha1
            )
            .is_ok()
        );
        // Valid SHA-256 hex (64 chars)
        assert!(validate_hex(&"a".repeat(64), ChecksumAlgorithm::Sha256).is_ok());
        // Valid ED2K hex (32 chars)
        assert!(validate_hex("31d6cfe0d16ae931b73c59d7e0c089c0", ChecksumAlgorithm::Ed2k).is_ok());
    }

    #[test]
    fn test_validate_hex_wrong_length() {
        // Too short for MD5
        let err = validate_hex("abc", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
        // Too long for SHA-1
        let err = validate_hex(&"a".repeat(41), ChecksumAlgorithm::Sha1).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
        // Too short for SHA-256
        let err = validate_hex(&"a".repeat(32), ChecksumAlgorithm::Sha256).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn test_validate_hex_non_hex_chars() {
        // 32 chars but with non-hex characters
        let err =
            validate_hex("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
        // Mixed valid and invalid
        let err =
            validate_hex("d41d8cd98f00b204e9800998ecf8427G", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn test_checksum_error_display() {
        let io_err = ChecksumError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "disk failure",
        ));
        assert!(format!("{}", io_err).contains("disk failure"));

        let hex_err = ChecksumError::InvalidHex("bad chars".into());
        assert!(format!("{}", hex_err).contains("bad chars"));

        let mismatch = ChecksumError::Mismatch {
            expected: "aaa".into(),
            actual: "bbb".into(),
        };
        let msg = format!("{}", mismatch);
        assert!(msg.contains("aaa"));
        assert!(msg.contains("bbb"));
        assert!(msg.contains("mismatch"));

        let unknown = ChecksumError::UnknownAlgorithm("blake2".into());
        assert!(format!("{}", unknown).contains("blake2"));
    }

    #[test]
    fn test_checksum_result_fields() {
        let result = ChecksumResult {
            algorithm: ChecksumAlgorithm::Md5,
            expected: "abc".into(),
            actual: "def".into(),
            matched: false,
        };
        assert_eq!(result.algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(result.expected, "abc");
        assert_eq!(result.actual, "def");
        assert!(!result.matched);
    }

    #[tokio::test]
    async fn test_md5_with_known_content() {
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
        assert_eq!(result.algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(result.expected, "5eb63bbbe01eeed093cb22bb8f5acdc3");
        assert_eq!(result.actual, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[tokio::test]
    async fn test_sha1_with_content() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hello world").unwrap();
        f.flush().unwrap();

        // SHA-1 of "hello world" = 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        let result = verify_file(
            f.path(),
            "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
            ChecksumAlgorithm::Sha1,
        )
        .await
        .unwrap();
        assert!(result.matched);
    }

    #[tokio::test]
    async fn test_sha256_with_content() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hello world").unwrap();
        f.flush().unwrap();

        // SHA-256 of "hello world" = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
        let result = verify_file(
            f.path(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            ChecksumAlgorithm::Sha256,
        )
        .await
        .unwrap();
        assert!(result.matched);
    }

    #[tokio::test]
    async fn test_ed2k_with_content() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hello world").unwrap();
        f.flush().unwrap();

        // ED2K hash of "hello world" fits in one chunk, so it's MD4 of "hello world"
        // First compute the hash, then verify it matches
        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);

        let result = verify_file(f.path(), &hash, ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert!(result.matched);
    }

    #[tokio::test]
    async fn test_verify_file_not_found() {
        let result = verify_file(
            std::path::Path::new("/nonexistent/file.txt"),
            "d41d8cd98f00b204e9800998ecf8427e",
            ChecksumAlgorithm::Md5,
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChecksumError::Io(_)));
    }

    #[tokio::test]
    async fn test_compute_hash_file_not_found() {
        let result = compute_hash(
            std::path::Path::new("/nonexistent/file.txt"),
            ChecksumAlgorithm::Md5,
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChecksumError::Io(_)));
    }

    #[tokio::test]
    async fn test_verify_file_normalizes_hex_to_lowercase() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hello world").unwrap();
        f.flush().unwrap();

        // Use uppercase hex - should still match
        let result = verify_file(
            f.path(),
            "5EB63BBBE01EEED093CB22BB8F5ACDC3",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();
        assert!(result.matched);
        // The expected field should be lowercased
        assert_eq!(result.expected, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[tokio::test]
    async fn test_compute_hash_all_algorithms_empty_file() {
        let f = NamedTempFile::new().unwrap();

        let ed2k = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(ed2k.len(), 32);
        assert_eq!(ed2k, "31d6cfe0d16ae931b73c59d7e0c089c0");
    }

    #[tokio::test]
    async fn test_compute_hash_binary_content() {
        let mut f = NamedTempFile::new().unwrap();
        // Write binary content (all byte values 0-255)
        let data: Vec<u8> = (0..=255).collect();
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        // Just verify it doesn't error and returns correct length
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

        let ed2k = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(ed2k.len(), 32);
    }

    #[tokio::test]
    async fn test_verify_file_mismatch_returns_correct_fields() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "test content").unwrap();
        f.flush().unwrap();

        let result = verify_file(
            f.path(),
            "00000000000000000000000000000000",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();

        assert!(!result.matched);
        assert_eq!(result.expected, "00000000000000000000000000000000");
        assert_ne!(result.actual, "00000000000000000000000000000000");
        assert_eq!(result.actual.len(), 32); // MD5 produces 32 hex chars
    }

    #[tokio::test]
    async fn test_ed2k_multi_chunk() {
        // Create a file larger than one ED2K chunk (9,728,000 bytes)
        // We'll use a smaller test: just verify the multi-chunk path works
        // by creating a file of exactly CHUNK_SIZE + 1 bytes
        let mut f = NamedTempFile::new().unwrap();
        let chunk_size = 9_728_000;
        let data = vec![0xABu8; chunk_size + 1];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        // Should use multi-chunk path (file > CHUNK_SIZE)
        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);

        // Verify it's different from a single-chunk hash
        let mut f2 = NamedTempFile::new().unwrap();
        let data2 = vec![0xABu8; chunk_size];
        f2.write_all(&data2).unwrap();
        f2.flush().unwrap();
        let hash2 = compute_hash(f2.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_ne!(
            hash, hash2,
            "Different size files should have different hashes"
        );
    }

    #[tokio::test]
    async fn test_ed2k_exact_chunk_boundary() {
        // File of exactly CHUNK_SIZE bytes = single chunk
        let mut f = NamedTempFile::new().unwrap();
        let chunk_size = 9_728_000;
        let data = vec![0xCDu8; chunk_size];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);

        // This should be the same as MD4 of the data (single chunk)
        // The hash should equal the chunk hash since there's only one chunk
    }

    #[tokio::test]
    async fn test_verify_consistency_across_calls() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "consistent content").unwrap();
        f.flush().unwrap();

        let result1 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        let result2 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(result1, result2, "Same file should produce same hash");
    }

    #[tokio::test]
    async fn test_different_content_different_hash() {
        let mut f1 = NamedTempFile::new().unwrap();
        write!(f1, "content A").unwrap();
        f1.flush().unwrap();

        let mut f2 = NamedTempFile::new().unwrap();
        write!(f2, "content B").unwrap();
        f2.flush().unwrap();

        let hash1 = compute_hash(f1.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        let hash2 = compute_hash(f2.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert_ne!(
            hash1, hash2,
            "Different content should produce different hashes"
        );
    }

    #[tokio::test]
    async fn test_large_file_sha256() {
        let mut f = NamedTempFile::new().unwrap();
        // Write 1MB of data
        let data = vec![0x42u8; 1024 * 1024];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(hash.len(), 64);
        // Verify it's all hex chars
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ─── Phase 241: Comprehensive Test Coverage ───

    // === ChecksumAlgorithm: serde snake_case values ===

    #[test]
    fn algorithm_serde_snake_case_values() {
        // Verify serde produces expected JSON string values
        let md5_json = serde_json::to_string(&ChecksumAlgorithm::Md5).unwrap();
        assert!(md5_json.contains("Md5") || md5_json.contains("\"Md5\""));

        let sha1_json = serde_json::to_string(&ChecksumAlgorithm::Sha1).unwrap();
        assert!(sha1_json.contains("Sha1"));

        let sha256_json = serde_json::to_string(&ChecksumAlgorithm::Sha256).unwrap();
        assert!(sha256_json.contains("Sha256"));

        let ed2k_json = serde_json::to_string(&ChecksumAlgorithm::Ed2k).unwrap();
        assert!(ed2k_json.contains("Ed2k"));
    }

    // === ChecksumAlgorithm: Eq / Hash traits ===

    #[test]
    fn algorithm_eq_trait() {
        assert_eq!(ChecksumAlgorithm::Md5, ChecksumAlgorithm::Md5);
        assert_eq!(ChecksumAlgorithm::Sha1, ChecksumAlgorithm::Sha1);
        assert_eq!(ChecksumAlgorithm::Sha256, ChecksumAlgorithm::Sha256);
        assert_eq!(ChecksumAlgorithm::Ed2k, ChecksumAlgorithm::Ed2k);
        assert_ne!(ChecksumAlgorithm::Md5, ChecksumAlgorithm::Sha1);
        assert_ne!(ChecksumAlgorithm::Md5, ChecksumAlgorithm::Ed2k);
        assert_ne!(ChecksumAlgorithm::Sha1, ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn algorithm_hash_trait() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ChecksumAlgorithm::Md5);
        set.insert(ChecksumAlgorithm::Sha1);
        set.insert(ChecksumAlgorithm::Sha256);
        set.insert(ChecksumAlgorithm::Ed2k);
        assert_eq!(set.len(), 4);
        // Duplicate should not increase size
        set.insert(ChecksumAlgorithm::Md5);
        assert_eq!(set.len(), 4);
        assert!(set.contains(&ChecksumAlgorithm::Md5));
        assert!(set.contains(&ChecksumAlgorithm::Sha256));
    }

    // === ChecksumAlgorithm: Copy trait ===

    #[test]
    fn algorithm_copy_trait() {
        let algo = ChecksumAlgorithm::Sha256;
        let copy = algo; // Copy
        assert_eq!(algo, copy);
        // Original still usable (proves Copy, not just Clone)
        assert_eq!(algo.name(), "SHA-256");
    }

    // === ChecksumAlgorithm: parse all aliases ===

    #[test]
    fn parse_all_md5_aliases() {
        assert_eq!(
            ChecksumAlgorithm::parse("md5"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("MD5"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Md5"),
            Some(ChecksumAlgorithm::Md5)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("mD5"),
            Some(ChecksumAlgorithm::Md5)
        );
    }

    #[test]
    fn parse_all_sha1_aliases() {
        assert_eq!(
            ChecksumAlgorithm::parse("sha1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("sha-1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA-1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Sha-1"),
            Some(ChecksumAlgorithm::Sha1)
        );
    }

    #[test]
    fn parse_all_sha256_aliases() {
        assert_eq!(
            ChecksumAlgorithm::parse("sha256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("sha-256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA-256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Sha-256"),
            Some(ChecksumAlgorithm::Sha256)
        );
    }

    #[test]
    fn parse_all_ed2k_aliases() {
        assert_eq!(
            ChecksumAlgorithm::parse("ed2k"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("ED2K"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Ed2k"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("ed2k-hash"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("ED2K-HASH"),
            Some(ChecksumAlgorithm::Ed2k)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("Ed2k-Hash"),
            Some(ChecksumAlgorithm::Ed2k)
        );
    }

    #[test]
    fn parse_rejects_invalid() {
        assert_eq!(ChecksumAlgorithm::parse(""), None);
        assert_eq!(ChecksumAlgorithm::parse(" "), None);
        assert_eq!(ChecksumAlgorithm::parse("sha512"), None);
        assert_eq!(ChecksumAlgorithm::parse("crc32"), None);
        assert_eq!(ChecksumAlgorithm::parse("blake2b"), None);
        assert_eq!(ChecksumAlgorithm::parse("md4"), None);
        assert_eq!(ChecksumAlgorithm::parse("sha-512"), None);
        assert_eq!(ChecksumAlgorithm::parse("md5 "), None); // trailing space
        assert_eq!(ChecksumAlgorithm::parse(" md5"), None); // leading space
    }

    // === ChecksumAlgorithm: name() exact values ===

    #[test]
    fn algorithm_name_exact_values() {
        assert_eq!(ChecksumAlgorithm::Md5.name(), "MD5");
        assert_eq!(ChecksumAlgorithm::Sha1.name(), "SHA-1");
        assert_eq!(ChecksumAlgorithm::Sha256.name(), "SHA-256");
        assert_eq!(ChecksumAlgorithm::Ed2k.name(), "ED2K");
    }

    // === ChecksumAlgorithm: hex_len() exact values ===

    #[test]
    fn algorithm_hex_len_exact_values() {
        assert_eq!(ChecksumAlgorithm::Md5.hex_len(), 32);
        assert_eq!(ChecksumAlgorithm::Sha1.hex_len(), 40);
        assert_eq!(ChecksumAlgorithm::Sha256.hex_len(), 64);
        assert_eq!(ChecksumAlgorithm::Ed2k.hex_len(), 32);
    }

    // === ChecksumError: From<io::Error> conversion ===

    #[test]
    fn error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let checksum_err: ChecksumError = io_err.into();
        let msg = format!("{}", checksum_err);
        assert!(msg.contains("access denied"));
        assert!(matches!(checksum_err, ChecksumError::Io(_)));
    }

    // === ChecksumError: Debug trait ===

    #[test]
    fn error_debug_trait() {
        let io_err = ChecksumError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let debug = format!("{:?}", io_err);
        assert!(debug.contains("Io"));

        let hex_err = ChecksumError::InvalidHex("bad".into());
        let debug = format!("{:?}", hex_err);
        assert!(debug.contains("InvalidHex"));

        let mismatch = ChecksumError::Mismatch {
            expected: "aaa".into(),
            actual: "bbb".into(),
        };
        let debug = format!("{:?}", mismatch);
        assert!(debug.contains("Mismatch"));

        let unknown = ChecksumError::UnknownAlgorithm("xyz".into());
        let debug = format!("{:?}", unknown);
        assert!(debug.contains("UnknownAlgorithm"));
    }

    // === ChecksumError: Display exact messages ===

    #[test]
    fn error_display_io() {
        let err = ChecksumError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "no file"));
        let msg = format!("{}", err);
        assert!(msg.contains("IO error"));
        assert!(msg.contains("no file"));
    }

    #[test]
    fn error_display_invalid_hex() {
        let err = ChecksumError::InvalidHex("too short".into());
        let msg = format!("{}", err);
        assert!(msg.contains("invalid hex string"));
        assert!(msg.contains("too short"));
    }

    #[test]
    fn error_display_mismatch() {
        let err = ChecksumError::Mismatch {
            expected: "abcdef".into(),
            actual: "123456".into(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("checksum mismatch"));
        assert!(msg.contains("abcdef"));
        assert!(msg.contains("123456"));
        assert!(msg.contains("expected"));
        assert!(msg.contains("got"));
    }

    #[test]
    fn error_display_unknown_algorithm() {
        let err = ChecksumError::UnknownAlgorithm("whirlpool".into());
        let msg = format!("{}", err);
        assert!(msg.contains("unknown algorithm"));
        assert!(msg.contains("whirlpool"));
    }

    // === ChecksumResult: Clone trait ===

    #[test]
    fn checksum_result_clone() {
        let result = ChecksumResult {
            algorithm: ChecksumAlgorithm::Sha256,
            expected: "abc".into(),
            actual: "def".into(),
            matched: false,
        };
        let cloned = result.clone();
        assert_eq!(cloned.algorithm, ChecksumAlgorithm::Sha256);
        assert_eq!(cloned.expected, "abc");
        assert_eq!(cloned.actual, "def");
        assert!(!cloned.matched);
    }

    #[test]
    fn checksum_result_clone_independence() {
        let result = ChecksumResult {
            algorithm: ChecksumAlgorithm::Md5,
            expected: "original".into(),
            actual: "hash".into(),
            matched: true,
        };
        let mut cloned = result.clone();
        cloned.expected = "modified".into();
        // Original unchanged
        assert_eq!(result.expected, "original");
        assert_eq!(cloned.expected, "modified");
    }

    // === validate_hex: boundary cases ===

    #[test]
    fn validate_hex_empty_string() {
        let err = validate_hex("", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn validate_hex_one_char_short() {
        // 31 chars for MD5 (needs 32)
        let err = validate_hex(&"a".repeat(31), ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn validate_hex_one_char_long() {
        // 33 chars for MD5 (needs 32)
        let err = validate_hex(&"a".repeat(33), ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn validate_hex_all_valid_hex_chars() {
        // All valid hex digits: 0-9, a-f, A-F
        assert!(validate_hex("0123456789abcdef0123456789ABCDEF", ChecksumAlgorithm::Md5).is_ok());
    }

    #[test]
    fn validate_hex_space_in_middle() {
        let err =
            validate_hex("d41d8cd98f00b204e9800998ecf8427 ", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn validate_hex_special_chars() {
        let err =
            validate_hex("d41d8cd98f00b204e9800998ecf8427!", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    #[test]
    fn validate_hex_unicode_rejected() {
        // 32 chars but contains Unicode
        let err =
            validate_hex("d41d8cd98f00b204e9800998ecf8427é", ChecksumAlgorithm::Md5).unwrap_err();
        assert!(matches!(err, ChecksumError::InvalidHex(_)));
    }

    // === detect_algorithm: comprehensive ===

    #[test]
    fn detect_algorithm_all_lengths() {
        // Length 32 → MD5 (or ED2K, defaults to MD5)
        assert_eq!(
            detect_algorithm(&"0".repeat(32)),
            Some(ChecksumAlgorithm::Md5)
        );
        // Length 40 → SHA-1
        assert_eq!(
            detect_algorithm(&"0".repeat(40)),
            Some(ChecksumAlgorithm::Sha1)
        );
        // Length 64 → SHA-256
        assert_eq!(
            detect_algorithm(&"0".repeat(64)),
            Some(ChecksumAlgorithm::Sha256)
        );
    }

    #[test]
    fn detect_algorithm_none_for_unrecognized_lengths() {
        for len in [0, 1, 10, 31, 33, 39, 41, 50, 63, 65, 100, 128] {
            assert_eq!(detect_algorithm(&"a".repeat(len)), None);
        }
    }

    // === verify_file: all algorithms empty file ===

    #[tokio::test]
    async fn verify_all_algorithms_empty_file() {
        let f = NamedTempFile::new().unwrap();

        // MD5 of empty = d41d8cd98f00b204e9800998ecf8427e
        let r = verify_file(
            f.path(),
            "d41d8cd98f00b204e9800998ecf8427e",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();
        assert!(r.matched);

        // SHA-1 of empty = da39a3ee5e6b4b0d3255bfef95601890afd80709
        let r = verify_file(
            f.path(),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            ChecksumAlgorithm::Sha1,
        )
        .await
        .unwrap();
        assert!(r.matched);

        // SHA-256 of empty = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let r = verify_file(
            f.path(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ChecksumAlgorithm::Sha256,
        )
        .await
        .unwrap();
        assert!(r.matched);

        // ED2K of empty = 31d6cfe0d16ae931b73c59d7e0c089c0
        let r = verify_file(
            f.path(),
            "31d6cfe0d16ae931b73c59d7e0c089c0",
            ChecksumAlgorithm::Ed2k,
        )
        .await
        .unwrap();
        assert!(r.matched);
    }

    // === verify_file: Unicode filename ===

    #[tokio::test]
    async fn verify_unicode_filename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("测试文件.txt");
        std::fs::write(&path, b"test content").unwrap();

        let hash = compute_hash(&path, ChecksumAlgorithm::Md5).await.unwrap();
        let result = verify_file(&path, &hash, ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert!(result.matched);
    }

    #[tokio::test]
    async fn verify_emoji_filename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("🚀download.txt");
        std::fs::write(&path, b"emoji file").unwrap();

        let hash = compute_hash(&path, ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        let result = verify_file(&path, &hash, ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert!(result.matched);
    }

    // === verify_file: hex normalization ===

    #[tokio::test]
    async fn verify_hex_normalization_mixed_case() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "test").unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        // Convert to mixed case
        let mixed: String = hash
            .chars()
            .enumerate()
            .map(|(i, c)| {
                if i % 2 == 0 {
                    c.to_uppercase().next().unwrap()
                } else {
                    c
                }
            })
            .collect();

        let result = verify_file(f.path(), &mixed, ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert!(result.matched);
        // Expected should be lowercased
        assert_eq!(result.expected, hash);
    }

    // === verify_file: mismatch details ===

    #[tokio::test]
    async fn verify_mismatch_contains_algorithm_info() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "data").unwrap();
        f.flush().unwrap();

        let result = verify_file(
            f.path(),
            "ffffffffffffffffffffffffffffffff",
            ChecksumAlgorithm::Md5,
        )
        .await
        .unwrap();

        assert!(!result.matched);
        assert_eq!(result.algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(result.expected, "ffffffffffffffffffffffffffffffff");
        assert_ne!(result.actual, "ffffffffffffffffffffffffffffffff");
    }

    // === compute_hash: determinism ===

    #[tokio::test]
    async fn compute_hash_deterministic_md5() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "deterministic test").unwrap();
        f.flush().unwrap();

        let h1 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        let h2 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        let h3 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
    }

    #[tokio::test]
    async fn compute_hash_deterministic_sha1() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "sha1 deterministic").unwrap();
        f.flush().unwrap();

        let h1 = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        let h2 = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn compute_hash_deterministic_sha256() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "sha256 deterministic").unwrap();
        f.flush().unwrap();

        let h1 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        let h2 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn compute_hash_deterministic_ed2k() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "ed2k deterministic").unwrap();
        f.flush().unwrap();

        let h1 = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        let h2 = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(h1, h2);
    }

    // === compute_hash: all produce hex-only output ===

    #[tokio::test]
    async fn compute_hash_output_is_hex() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hex output test").unwrap();
        f.flush().unwrap();

        for algo in [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Ed2k,
        ] {
            let hash = compute_hash(f.path(), algo).await.unwrap();
            assert!(
                hash.chars().all(|c| c.is_ascii_hexdigit()),
                "Non-hex char in {:?} hash: {}",
                algo,
                hash
            );
        }
    }

    // === compute_hash: different algorithms produce different hashes ===

    #[tokio::test]
    async fn compute_hash_different_algorithms_different_output() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "different algorithms").unwrap();
        f.flush().unwrap();

        let md5 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        let sha1 = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        let sha256 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        let ed2k = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();

        // All different lengths (except MD5/ED2K which are same length but different values)
        assert_eq!(md5.len(), 32);
        assert_eq!(sha1.len(), 40);
        assert_eq!(sha256.len(), 64);
        assert_eq!(ed2k.len(), 32);
        // MD5 and ED2K same length but different values
        assert_ne!(md5, ed2k);
    }

    // === ED2K: multi-chunk scenarios ===

    #[tokio::test]
    async fn ed2k_two_chunks() {
        // File of exactly 2 * CHUNK_SIZE bytes
        let mut f = NamedTempFile::new().unwrap();
        let chunk_size = 9_728_000;
        let data = vec![0x42u8; chunk_size * 2];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);

        // Verify determinism
        let hash2 = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash, hash2);
    }

    #[tokio::test]
    async fn ed2k_chunk_plus_one_byte() {
        // File of CHUNK_SIZE + 1 bytes → 2 chunks (one full, one with 1 byte)
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0xFFu8; 9_728_001];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);
    }

    #[tokio::test]
    async fn ed2k_three_chunks() {
        // File of 3 * CHUNK_SIZE bytes
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0x11u8; 9_728_000 * 3];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);
    }

    #[tokio::test]
    async fn ed2k_small_file_single_chunk() {
        // Small file (100 bytes) → single chunk, hash = MD4(data)
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0xAAu8; 100];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash.len(), 32);

        // Same content → same hash
        let mut f2 = NamedTempFile::new().unwrap();
        f2.write_all(&data).unwrap();
        f2.flush().unwrap();
        let hash2 = compute_hash(f2.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(hash, hash2);
    }

    #[tokio::test]
    async fn ed2k_different_content_different_hash() {
        let mut f1 = NamedTempFile::new().unwrap();
        write!(f1, "content A for ed2k").unwrap();
        f1.flush().unwrap();

        let mut f2 = NamedTempFile::new().unwrap();
        write!(f2, "content B for ed2k").unwrap();
        f2.flush().unwrap();

        let h1 = compute_hash(f1.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        let h2 = compute_hash(f2.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_ne!(h1, h2);
    }

    // === verify_file: single byte file ===

    #[tokio::test]
    async fn verify_single_byte_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&[0x00]).unwrap();
        f.flush().unwrap();

        // Compute hash first, then verify
        let hash = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        let result = verify_file(f.path(), &hash, ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert!(result.matched);
    }

    // === verify_file: large file all algorithms ===

    #[tokio::test]
    async fn verify_large_file_all_algorithms() {
        let mut f = NamedTempFile::new().unwrap();
        // Write 512KB of data
        let data = vec![0x55u8; 512 * 1024];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        for algo in [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Ed2k,
        ] {
            let hash = compute_hash(f.path(), algo).await.unwrap();
            let result = verify_file(f.path(), &hash, algo).await.unwrap();
            assert!(result.matched, "{:?} should match for large file", algo);
        }
    }

    // === compute_hash: file not found ===

    #[tokio::test]
    async fn compute_hash_nonexistent_file_all_algos() {
        let path = std::path::Path::new("/nonexistent/path/file.bin");
        for algo in [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Ed2k,
        ] {
            let result = compute_hash(path, algo).await;
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), ChecksumError::Io(_)));
        }
    }

    // === verify_file: invalid hex before file read ===

    #[tokio::test]
    async fn verify_invalid_hex_length_returns_error() {
        let f = NamedTempFile::new().unwrap();
        // Too short for SHA-256
        let result = verify_file(f.path(), "abcd", ChecksumAlgorithm::Sha256).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChecksumError::InvalidHex(_)));
    }

    #[tokio::test]
    async fn verify_invalid_hex_chars_returns_error() {
        let f = NamedTempFile::new().unwrap();
        // 64 chars but non-hex
        let result = verify_file(f.path(), &"z".repeat(64), ChecksumAlgorithm::Sha256).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChecksumError::InvalidHex(_)));
    }

    // === validate_hex: error message content ===

    #[test]
    fn validate_hex_error_message_contains_expected_length() {
        let err = validate_hex("abc", ChecksumAlgorithm::Sha256).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("64")); // SHA-256 expects 64 hex chars
        assert!(msg.contains("SHA-256"));
        assert!(msg.contains("3")); // got 3
    }

    // === Multiple verifications on same file ===

    #[tokio::test]
    async fn verify_same_file_multiple_algorithms() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "multi-algo test content").unwrap();
        f.flush().unwrap();

        let md5_hash = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        let sha1_hash = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();

        let r1 = verify_file(f.path(), &md5_hash, ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert!(r1.matched);

        let r2 = verify_file(f.path(), &sha1_hash, ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        assert!(r2.matched);

        // Cross-verify should fail (MD5 hash ≠ SHA-1 hash)
        let r3 = verify_file(f.path(), &md5_hash, ChecksumAlgorithm::Sha1).await;
        // This will fail because MD5 is 32 chars but SHA-1 expects 40
        assert!(r3.is_err());
    }

    // === Empty file hashes are well-known ===

    #[tokio::test]
    async fn empty_file_known_hashes() {
        let f = NamedTempFile::new().unwrap();

        let md5 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert_eq!(md5, "d41d8cd98f00b204e9800998ecf8427e");

        let sha1 = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        assert_eq!(sha1, "da39a3ee5e6b4b0d3255bfef95601890afd80709");

        let sha256 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(
            sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let ed2k = compute_hash(f.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();
        assert_eq!(ed2k, "31d6cfe0d16ae931b73c59d7e0c089c0");
    }

    // === "hello world" known hashes ===

    #[tokio::test]
    async fn hello_world_known_hashes() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "hello world").unwrap();
        f.flush().unwrap();

        let md5 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert_eq!(md5, "5eb63bbbe01eeed093cb22bb8f5acdc3");

        let sha1 = compute_hash(f.path(), ChecksumAlgorithm::Sha1)
            .await
            .unwrap();
        assert_eq!(sha1, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");

        let sha256 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(
            sha256,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    // === Algorithm all variants iteration ===

    #[test]
    fn algorithm_all_variants_have_name() {
        let algos = [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Ed2k,
        ];
        for algo in &algos {
            assert!(!algo.name().is_empty());
            assert!(algo.hex_len() > 0);
        }
    }

    // === Serde: pretty format ===

    #[test]
    fn algorithm_serde_pretty() {
        let algo = ChecksumAlgorithm::Sha256;
        let pretty = serde_json::to_string_pretty(&algo).unwrap();
        let deserialized: ChecksumAlgorithm = serde_json::from_str(&pretty).unwrap();
        assert_eq!(algo, deserialized);
    }

    // === ChecksumResult: Debug trait ===

    #[test]
    fn checksum_result_debug() {
        let result = ChecksumResult {
            algorithm: ChecksumAlgorithm::Md5,
            expected: "abc123".into(),
            actual: "def456".into(),
            matched: false,
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("ChecksumResult"));
        assert!(debug.contains("abc123"));
        assert!(debug.contains("def456"));
    }

    // === Binary content known hashes ===

    #[tokio::test]
    async fn binary_all_zeros_md5() {
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0u8; 1024];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        // MD5 of 1024 zero bytes is deterministic
        let hash2 = compute_hash(f.path(), ChecksumAlgorithm::Md5)
            .await
            .unwrap();
        assert_eq!(hash, hash2);
        assert_eq!(hash.len(), 32);
    }

    #[tokio::test]
    async fn binary_all_ff_sha256() {
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0xFFu8; 2048];
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let hash = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(hash.len(), 64);
        // Verify deterministic
        let hash2 = compute_hash(f.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(hash, hash2);
    }

    // === ED2K: exact chunk boundary produces same hash as single chunk ===

    #[tokio::test]
    async fn ed2k_exact_chunk_vs_smaller_file() {
        // Exactly CHUNK_SIZE bytes → single chunk
        let mut f1 = NamedTempFile::new().unwrap();
        let data1 = vec![0x55u8; 9_728_000];
        f1.write_all(&data1).unwrap();
        f1.flush().unwrap();
        let hash1 = compute_hash(f1.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();

        // CHUNK_SIZE - 1 bytes → still single chunk (but different content)
        let mut f2 = NamedTempFile::new().unwrap();
        let data2 = vec![0x55u8; 9_727_999];
        f2.write_all(&data2).unwrap();
        f2.flush().unwrap();
        let hash2 = compute_hash(f2.path(), ChecksumAlgorithm::Ed2k)
            .await
            .unwrap();

        // Different sizes → different hashes
        assert_ne!(hash1, hash2);
    }
}
