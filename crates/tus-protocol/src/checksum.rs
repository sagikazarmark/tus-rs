//! Checksum calculation utilities.
//!
//! This module provides functions for calculating and verifying checksums
//! for the TUS Checksum extension.

use crate::config::ChecksumAlgorithm;

/// A streaming checksum calculator that can be updated incrementally.
#[cfg(feature = "checksum")]
pub struct StreamingChecksum {
    algorithm: ChecksumAlgorithm,
    state: ChecksumState,
}

#[cfg(feature = "checksum")]
impl StreamingChecksum {
    /// Creates a new streaming checksum calculator.
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        use sha1::Digest as _;

        let state = match algorithm {
            ChecksumAlgorithm::Sha1 => ChecksumState::Sha1(sha1::Sha1::new()),
            ChecksumAlgorithm::Sha256 => ChecksumState::Sha256(sha2::Sha256::new()),
            ChecksumAlgorithm::Md5 => ChecksumState::Md5(md5::Context::new()),
            ChecksumAlgorithm::Crc32 => ChecksumState::Crc32(crc32fast::Hasher::new()),
        };

        Self { algorithm, state }
    }

    /// Updates the checksum with more data.
    pub fn update(&mut self, data: &[u8]) {
        use sha1::Digest as _;

        match &mut self.state {
            ChecksumState::Sha1(hasher) => hasher.update(data),
            ChecksumState::Sha256(hasher) => hasher.update(data),
            ChecksumState::Md5(ctx) => ctx.consume(data),
            ChecksumState::Crc32(hasher) => hasher.update(data),
        }
    }

    /// Finalizes and returns the checksum.
    pub fn finalize(self) -> Vec<u8> {
        use sha1::Digest as _;

        match self.state {
            ChecksumState::Sha1(hasher) => hasher.finalize().to_vec(),
            ChecksumState::Sha256(hasher) => hasher.finalize().to_vec(),
            ChecksumState::Md5(ctx) => ctx.compute().to_vec(),
            ChecksumState::Crc32(hasher) => hasher.finalize().to_be_bytes().to_vec(),
        }
    }

    /// Returns the algorithm being used.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }
}

/// Calculates a checksum for the given data using the specified algorithm.
#[cfg(feature = "checksum")]
pub fn calculate(algorithm: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    match algorithm {
        ChecksumAlgorithm::Sha1 => {
            use sha1::{Digest, Sha1};
            let mut hasher = Sha1::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        ChecksumAlgorithm::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        ChecksumAlgorithm::Md5 => {
            let digest = md5::compute(data);
            digest.to_vec()
        }
        ChecksumAlgorithm::Crc32 => {
            let hash = crc32fast::hash(data);
            hash.to_be_bytes().to_vec()
        }
    }
}

/// Verifies that the calculated checksum matches the expected checksum.
#[cfg(feature = "checksum")]
pub fn verify(algorithm: ChecksumAlgorithm, data: &[u8], expected: &[u8]) -> bool {
    let calculated = calculate(algorithm, data);
    calculated == expected
}

#[cfg(feature = "checksum")]
enum ChecksumState {
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
    Md5(md5::Context),
    Crc32(crc32fast::Hasher),
}

#[cfg(all(test, feature = "checksum"))]
mod tests {
    use super::*;

    #[test]
    fn test_sha1() {
        let data = b"hello world";
        let result = calculate(ChecksumAlgorithm::Sha1, data);
        // SHA1 produces 20 bytes
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn test_sha256() {
        let data = b"hello world";
        let result = calculate(ChecksumAlgorithm::Sha256, data);
        // SHA256 produces 32 bytes
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_md5() {
        let data = b"hello world";
        let result = calculate(ChecksumAlgorithm::Md5, data);
        // MD5 produces 16 bytes
        assert_eq!(result.len(), 16);
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let result = calculate(ChecksumAlgorithm::Crc32, data);
        // CRC32 produces 4 bytes
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_verify() {
        let data = b"hello world";
        let checksum = calculate(ChecksumAlgorithm::Md5, data);
        assert!(verify(ChecksumAlgorithm::Md5, data, &checksum));
        assert!(!verify(ChecksumAlgorithm::Md5, data, b"wrong"));
    }

    #[test]
    fn test_streaming() {
        let mut hasher = StreamingChecksum::new(ChecksumAlgorithm::Sha256);
        hasher.update(b"hello ");
        hasher.update(b"world");
        let result = hasher.finalize();

        let direct = calculate(ChecksumAlgorithm::Sha256, b"hello world");
        assert_eq!(result, direct);
    }

    #[test]
    fn test_consistency() {
        // Verify that calculating the same data twice gives the same result
        let data = b"test data for checksum";

        for algorithm in [
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Crc32,
        ] {
            let result1 = calculate(algorithm, data);
            let result2 = calculate(algorithm, data);
            assert_eq!(
                result1, result2,
                "Checksum should be deterministic for {:?}",
                algorithm
            );
        }
    }

    #[test]
    fn test_sha1_known_value() {
        // Verify SHA1 implementation against known value
        // SHA1("hello world") = 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed (hex)
        let data = b"hello world";
        let result = calculate(ChecksumAlgorithm::Sha1, data);

        // Convert to hex for comparison
        let hex_result: String = result.iter().map(|b| format!("{:02x}", b)).collect();
        println!("SHA1(\"hello world\") hex: {}", hex_result);

        assert_eq!(
            hex_result, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
            "SHA1 of 'hello world' should match known value"
        );
    }

    #[test]
    fn test_sha1_hello_tus_variants() {
        // Test case from hurl test
        // The test comment says: SHA1 of "Hello, tus!\n" = jiVMcGsdc+pakQJdI4OKm+zKT3s=
        // Let's compute various possibilities to find the correct data
        use base64::Engine;

        let expected_b64 = "jiVMcGsdc+pakQJdI4OKm+zKT3s=";

        // Test various data possibilities
        let variants = [
            ("Hello, tus!", b"Hello, tus!".as_slice()),
            ("Hello, tus!\\n", b"Hello, tus!\n".as_slice()),
            ("Hello, tus!\\r\\n", b"Hello, tus!\r\n".as_slice()),
            ("Hello, tus! ", b"Hello, tus! ".as_slice()),
        ];

        for (name, data) in variants {
            let result = calculate(ChecksumAlgorithm::Sha1, data);
            let base64_result = base64::engine::general_purpose::STANDARD.encode(&result);
            println!(
                "{:20} len={:2} => {}{}",
                name,
                data.len(),
                base64_result,
                if base64_result == expected_b64 {
                    " <-- MATCH!"
                } else {
                    ""
                }
            );
        }

        println!("\nExpected: {}", expected_b64);
    }
}
