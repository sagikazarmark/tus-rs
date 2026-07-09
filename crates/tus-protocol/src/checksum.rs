//! Checksum calculation utilities.
//!
//! This module provides functions for calculating and verifying checksums
//! for the TUS Checksum extension.

use crate::config::ChecksumAlgorithm;

/// Calculates a checksum for the given data using the specified algorithm.
#[cfg(feature = "checksum")]
pub fn calculate(algorithm: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new(algorithm);
    hasher.update(data);
    hasher.finalize()
}

/// Incrementally computes a checksum over streamed body chunks.
///
/// Use this instead of [`calculate`] when the body is fed in chunks rather than
/// as one slice: the hasher's own state stays constant regardless of body size.
/// This bounds only the digest state — it does not imply the surrounding intake
/// is constant-memory. A body of unknown length (chunked transfer / no
/// `Content-Length`) is still buffered whole to discover its size, bounded by
/// [`Config::max_intake_buffer`](crate::Config::max_intake_buffer).
#[cfg(feature = "checksum")]
pub struct Hasher {
    inner: HasherInner,
}

#[cfg(feature = "checksum")]
enum HasherInner {
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
    Md5(md5::Context),
    Crc32(crc32fast::Hasher),
}

#[cfg(feature = "checksum")]
impl Hasher {
    /// Creates a hasher for the given algorithm.
    #[must_use]
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        use sha1::Digest as _;

        let inner = match algorithm {
            ChecksumAlgorithm::Sha1 => HasherInner::Sha1(sha1::Sha1::new()),
            ChecksumAlgorithm::Sha256 => HasherInner::Sha256(sha2::Sha256::new()),
            ChecksumAlgorithm::Md5 => HasherInner::Md5(md5::Context::new()),
            ChecksumAlgorithm::Crc32 => HasherInner::Crc32(crc32fast::Hasher::new()),
        };
        Self { inner }
    }

    /// Returns the algorithm this hasher computes.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match &self.inner {
            HasherInner::Sha1(_) => ChecksumAlgorithm::Sha1,
            HasherInner::Sha256(_) => ChecksumAlgorithm::Sha256,
            HasherInner::Md5(_) => ChecksumAlgorithm::Md5,
            HasherInner::Crc32(_) => ChecksumAlgorithm::Crc32,
        }
    }

    /// Feeds body bytes into the hasher.
    pub fn update(&mut self, data: &[u8]) {
        use sha1::Digest as _;

        match &mut self.inner {
            HasherInner::Sha1(hasher) => hasher.update(data),
            HasherInner::Sha256(hasher) => hasher.update(data),
            HasherInner::Md5(context) => context.consume(data),
            HasherInner::Crc32(hasher) => hasher.update(data),
        }
    }

    /// Consumes the hasher and returns the checksum bytes.
    #[must_use]
    pub fn finalize(self) -> Vec<u8> {
        use sha1::Digest as _;

        match self.inner {
            HasherInner::Sha1(hasher) => hasher.finalize().to_vec(),
            HasherInner::Sha256(hasher) => hasher.finalize().to_vec(),
            HasherInner::Md5(context) => context.finalize().to_vec(),
            HasherInner::Crc32(hasher) => hasher.finalize().to_be_bytes().to_vec(),
        }
    }
}

#[cfg(feature = "checksum")]
impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hasher")
            .field("algorithm", &self.algorithm())
            .finish()
    }
}

#[cfg(all(test, feature = "checksum"))]
mod tests {
    use super::*;

    #[test]
    fn digest_lengths_match_algorithms() {
        let data = b"hello world";
        assert_eq!(calculate(ChecksumAlgorithm::Sha1, data).len(), 20);
        assert_eq!(calculate(ChecksumAlgorithm::Sha256, data).len(), 32);
        assert_eq!(calculate(ChecksumAlgorithm::Md5, data).len(), 16);
        assert_eq!(calculate(ChecksumAlgorithm::Crc32, data).len(), 4);
    }

    #[test]
    fn calculation_is_deterministic() {
        let data = b"test data for checksum";

        for algorithm in [
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Crc32,
        ] {
            assert_eq!(
                calculate(algorithm, data),
                calculate(algorithm, data),
                "checksum should be deterministic for {algorithm:?}",
            );
        }
    }

    #[test]
    fn sha1_matches_known_value() {
        let result = calculate(ChecksumAlgorithm::Sha1, b"hello world");
        let hex_result: String = result.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex_result, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[test]
    fn incremental_hashing_matches_one_shot() {
        let data = b"incremental hashing must match one-shot hashing";

        for algorithm in [
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Crc32,
        ] {
            let mut hasher = Hasher::new(algorithm);
            for chunk in data.chunks(7) {
                hasher.update(chunk);
            }
            assert_eq!(
                hasher.finalize(),
                calculate(algorithm, data),
                "incremental digest should match for {algorithm:?}",
            );
        }
    }
}
