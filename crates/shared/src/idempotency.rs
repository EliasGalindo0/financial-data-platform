/// Idempotency handling.
///
/// Strategy:
///   1. Client supplies `Idempotency-Key` header (UUID v4, max 64 chars)
///   2. We hash the full canonical request body (SHA-256)
///   3. We do a single DB upsert that atomically inserts or returns existing
///   4. If returned row has different hash → the key was reused with different
///      payload → return 422 (key collision, client bug)
///   5. If returned row is PENDING/PROCESSING → return 202 (still working)
///   6. If returned row is SETTLED/FAILED → return the cached response
///
/// Why not Redis?
///   Redis is ephemeral. For financial transactions, we need durability.
///   PostgreSQL UNIQUE constraint gives us atomic check-and-insert for free.
///   Redis is used *additionally* for sub-millisecond hot-path caching of
///   recently processed keys (< 5 min TTL) to avoid DB round-trips on retries.

use sha2::{Digest, Sha256};
use std::fmt;

/// Compute SHA-256 of a canonical request body.
/// "Canonical" means: JSON with keys sorted alphabetically, no whitespace.
pub fn compute_request_hash(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    hex::encode(hasher.finalize())
}

/// Idempotency key wrapper — validated on ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(raw: impl Into<String>) -> Result<Self, IdempotencyKeyError> {
        let s = raw.into();
        if s.is_empty() {
            return Err(IdempotencyKeyError::Empty);
        }
        if s.len() > 64 {
            return Err(IdempotencyKeyError::TooLong(s.len()));
        }
        // Only allow URL-safe characters
        if !s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
            return Err(IdempotencyKeyError::InvalidChars);
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IdempotencyKeyError {
    #[error("idempotency key is empty")]
    Empty,
    #[error("idempotency key too long: {0} chars (max 64)")]
    TooLong(usize),
    #[error("idempotency key contains invalid characters (allowed: a-z, A-Z, 0-9, -, _)")]
    InvalidChars,
}
