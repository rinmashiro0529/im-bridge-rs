use sha2::{Digest, Sha256};

/// Hash the ordered locator fields using u64 big-endian UTF-8 byte lengths.
/// This encoding is persisted and authenticated; it must remain byte-stable.
pub fn locator_hash(handle: &str, avatar: &str, chat_file: &str) -> String {
    let mut encoded = Vec::new();
    for part in [handle, avatar, chat_file] {
        let bytes = part.as_bytes();
        encoded.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        encoded.extend_from_slice(bytes);
    }
    hex::encode(Sha256::digest(encoded))
}
