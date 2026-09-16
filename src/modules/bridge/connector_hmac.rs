use std::collections::HashMap;
use std::fmt;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

pub const ALLOWED_CONTENT_TYPE: &str = "application/octet-stream";
pub const MAX_SKEW_SECS: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorHmacRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub timestamp_unix: i64,
    pub nonce: &'a str,
    pub content_type: &'a str,
    pub body: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorHmacError {
    AmbiguousQuery,
    InvalidPath,
    MediaType,
    TimestampSkew,
    NonceReplay,
    InvalidNonce,
    InvalidKey,
    SignatureMismatch,
}

impl ConnectorHmacError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AmbiguousQuery => "ambiguous query",
            Self::InvalidPath => "invalid path",
            Self::MediaType => "unsupported media type",
            Self::TimestampSkew => "timestamp skew",
            Self::NonceReplay => "nonce replay",
            Self::InvalidNonce => "invalid nonce",
            Self::InvalidKey => "invalid hmac key",
            Self::SignatureMismatch => "signature mismatch",
        }
    }
}

impl fmt::Display for ConnectorHmacError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for ConnectorHmacError {}

#[derive(Debug, Default)]
pub struct MemoryNonceSet {
    inner: HashMap<String, i64>,
}

impl MemoryNonceSet {
    pub fn insert_if_fresh(&mut self, nonce: &str, ts: i64) -> Result<(), ConnectorHmacError> {
        if self.inner.contains_key(nonce) {
            return Err(ConnectorHmacError::NonceReplay);
        }
        self.inner.insert(nonce.to_string(), ts);
        Ok(())
    }

    pub fn prune_older_than(&mut self, min_ts: i64) {
        self.inner.retain(|_, ts| *ts >= min_ts);
    }

    fn contains(&self, nonce: &str) -> bool {
        self.inner.contains_key(nonce)
    }
}

pub fn body_sha256(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

pub fn normalize_query(query: &str) -> Result<String, ConnectorHmacError> {
    if query.is_empty() {
        return Ok(String::new());
    }
    if query
        .as_bytes()
        .iter()
        .any(|byte| *byte == b'#' || *byte == b' ')
    {
        return Err(ConnectorHmacError::AmbiguousQuery);
    }

    let mut pairs: Vec<(&str, &str)> = Vec::new();
    let mut seen = HashMap::<&str, ()>::new();
    for part in query.split('&') {
        if part.is_empty() {
            return Err(ConnectorHmacError::AmbiguousQuery);
        }
        let (key, value) = match part.split_once('=') {
            Some((key, value)) => (key, value),
            None => (part, ""),
        };
        if key.is_empty() || seen.insert(key, ()).is_some() {
            return Err(ConnectorHmacError::AmbiguousQuery);
        }
        pairs.push((key, value));
    }

    pairs.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    Ok(pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&"))
}

pub fn canonical_string(req: &ConnectorHmacRequest<'_>) -> Result<String, ConnectorHmacError> {
    if req.content_type != ALLOWED_CONTENT_TYPE {
        return Err(ConnectorHmacError::MediaType);
    }
    if !path_is_legal(req.path) {
        return Err(ConnectorHmacError::InvalidPath);
    }
    let normalized_query = normalize_query(req.query)?;
    let method = req.method.to_ascii_uppercase();
    Ok(format!(
        "{method}\n{path}\n{query}\n{timestamp}\n{nonce}\n{content_type}\n{body_hash}",
        path = req.path,
        query = normalized_query,
        timestamp = req.timestamp_unix,
        nonce = req.nonce,
        content_type = req.content_type,
        body_hash = body_sha256(req.body),
    ))
}

pub fn sign(key: &[u8], canonical: &str) -> Result<String, ConnectorHmacError> {
    if key.is_empty() {
        return Err(ConnectorHmacError::InvalidKey);
    }
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(key).map_err(|_| ConnectorHmacError::InvalidKey)?;
    mac.update(canonical.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn verify(
    key: &[u8],
    req: &ConnectorHmacRequest<'_>,
    provided_hex: &str,
    now_unix: i64,
    seen_nonces: &mut MemoryNonceSet,
) -> Result<(), ConnectorHmacError> {
    if req.content_type != ALLOWED_CONTENT_TYPE {
        return Err(ConnectorHmacError::MediaType);
    }
    if !path_is_legal(req.path) {
        return Err(ConnectorHmacError::InvalidPath);
    }
    let _normalized_query = normalize_query(req.query)?;
    if !nonce_is_valid(req.nonce) {
        return Err(ConnectorHmacError::InvalidNonce);
    }
    if req.timestamp_unix.abs_diff(now_unix) > MAX_SKEW_SECS as u64 {
        return Err(ConnectorHmacError::TimestampSkew);
    }
    if seen_nonces.contains(req.nonce) {
        return Err(ConnectorHmacError::NonceReplay);
    }

    let canonical = canonical_string(req)?;
    let expected = sign(key, &canonical)?;
    let provided = provided_hex.to_ascii_lowercase();
    if provided.len() != expected.len()
        || !bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
    {
        return Err(ConnectorHmacError::SignatureMismatch);
    }

    seen_nonces.insert_if_fresh(req.nonce, req.timestamp_unix)
}

fn path_is_legal(path: &str) -> bool {
    path.starts_with('/')
        && !path.contains("..")
        && !path.contains("//")
        && !path.contains('?')
        && !path.contains('#')
}

fn nonce_is_valid(nonce: &str) -> bool {
    let len = nonce.len();
    (8..=128).contains(&len)
        && nonce
            .bytes()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}
