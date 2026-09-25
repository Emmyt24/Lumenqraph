//! Cryptographic utilities for constant-time comparison of secrets and signatures.
//!
//! All functions in this module use constant-time comparison to prevent timing
//! attacks on sensitive data like HMAC signatures, API keys, and secrets.

use subtle::ConstantTimeEq;

/// Placeholder stored in the plaintext `secret` column once a secret has been
/// encrypted into `encrypted_secret`.
///
/// Migration `0020_webhook_secret_encryption_backfill.sql` attempted to encrypt
/// legacy plaintext secrets using `pgp_sym_encrypt(secret, current_setting('app.webhook_encryption_key', true))`,
/// but nothing ever sets that GUC, so `pgp_sym_encrypt(x, NULL)` returned `NULL`
/// and the backfill was a no-op. The backfill is therefore performed in
/// application code (where the key is available) using the SQL below.
pub const ENCRYPTED_SECRET_PLACEHOLDER: &str = "[encrypted]";

/// Idempotent SQL statement that encrypts legacy plaintext webhook secrets.
///
/// It encrypts every row whose `encrypted_secret` is still `NULL` and whose
/// plaintext `secret` is not already the `[encrypted]` placeholder, storing the
/// ciphertext in `encrypted_secret` and replacing the plaintext with the
/// placeholder. The `$1` parameter is the webhook encryption key.
///
/// Running this repeatedly is safe: once a row has been backfilled its
/// `encrypted_secret` is non-`NULL` and its `secret` is the placeholder, so it
/// no longer matches the `WHERE` clause.
pub const BACKFILL_WEBHOOK_SECRETS_SQL: &str = "UPDATE webhook_subscriptions \
     SET encrypted_secret = pgp_sym_encrypt(secret, $1), \
         secret = '[encrypted]' \
     WHERE encrypted_secret IS NULL \
       AND secret <> '[encrypted]'";

/// Verify an HMAC-SHA256 signature using constant-time comparison.
///
/// This function compares the provided signature with the expected signature
/// using a constant-time algorithm, preventing timing attacks.
///
/// # Arguments
///
/// * `expected` - The expected signature (e.g., from HMAC calculation)
/// * `provided` - The provided signature (e.g., from HTTP header)
///
/// # Returns
///
/// `true` if the signatures match (in constant time), `false` otherwise.
///
/// # Example
///
/// ```ignore
/// use lumenqraph_core::crypto::verify_hmac_signature;
/// use hmac::{Hmac, Mac};
/// use sha2::Sha256;
///
/// type HmacSha256 = Hmac<Sha256>;
///
/// let secret = b"my-secret";
/// let body = b"webhook payload";
///
/// let mut mac = HmacSha256::new_from_slice(secret).unwrap();
/// mac.update(body);
/// let expected = hex::encode(mac.finalize().into_bytes());
///
/// let provided = "sha256=abc123...";
/// if verify_hmac_signature(&expected, provided) {
///     // Signature is valid
/// } else {
///     // Signature is invalid
/// }
/// ```
pub fn verify_hmac_signature(expected: &str, provided: &str) -> bool {
    // Extract the hex part after "sha256=" if present
    let provided_hex = if let Some(hex) = provided.strip_prefix("sha256=") {
        hex
    } else {
        provided
    };

    // Use constant-time comparison on the hex strings
    bool::from(expected.as_bytes().ct_eq(provided_hex.as_bytes()))
}

/// Verify that two byte slices are equal using constant-time comparison.
///
/// This function is useful for comparing API keys, tokens, or other secrets
/// that have been hashed or encoded to bytes.
///
/// # Arguments
///
/// * `expected` - The expected bytes (e.g., from database)
/// * `provided` - The provided bytes (e.g., from request)
///
/// `true` if the bytes match (in constant time), `false` otherwise.
pub fn verify_bytes_equal(expected: &[u8], provided: &[u8]) -> bool {
    bool::from(expected.ct_eq(provided))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_hmac_signature_with_prefix() {
        let expected = "abc123def456";
        let provided = "sha256=abc123def456";
        assert!(verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_hmac_signature_without_prefix() {
        let expected = "abc123def456";
        let provided = "abc123def456";
        assert!(verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_hmac_signature_mismatch() {
        let expected = "abc123def456";
        let provided = "sha256=abc123def457"; // Last digit differs
        assert!(!verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_hmac_signature_missing_prefix() {
        let expected = "abc123def456";
        let provided = "sha256=different";
        assert!(!verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_bytes_equal() {
        let expected = b"secret123";
        let provided = b"secret123";
        assert!(verify_bytes_equal(expected, provided));
    }

    #[test]
    fn test_verify_bytes_equal_mismatch() {
        let expected = b"secret123";
        let provided = b"secret124";
        assert!(!verify_bytes_equal(expected, provided));
    }

    #[test]
    fn test_verify_bytes_equal_different_lengths() {
        let expected = b"secret";
        let provided = b"secret123";
        assert!(!verify_bytes_equal(expected, provided));
    }

    #[test]
    fn test_backfill_sql_is_idempotent_and_encrypts() {
        // The backfill must only touch rows that still have a NULL
        // encrypted_secret and a non-placeholder plaintext secret, so that
        // re-running it is a no-op.
        assert!(BACKFILL_WEBHOOK_SECRETS_SQL.contains("encrypted_secret IS NULL"));
        assert!(BACKFILL_WEBHOOK_SECRETS_SQL.contains("secret <> '[encrypted]'"));
        assert!(BACKFILL_WEBHOOK_SECRETS_SQL.contains("pgp_sym_encrypt(secret, $1)"));
        assert_eq!(ENCRYPTED_SECRET_PLACEHOLDER, "[encrypted]");
    }
}
