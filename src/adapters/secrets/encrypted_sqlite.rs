use async_trait::async_trait;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use zeroize::Zeroize;

use crate::clock::now_rfc3339;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::seams::secret_vault::{SecretMeta, SecretVault};

type HmacSha256 = Hmac<Sha256>;

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;

pub struct EncryptedSqliteVault {
    pool: SqlitePool,
    master_key: [u8; KEY_LEN],
    key_version: i64,
}

impl EncryptedSqliteVault {
    pub fn new(pool: SqlitePool, master_key: [u8; KEY_LEN]) -> Self {
        Self {
            pool,
            master_key,
            key_version: 1,
        }
    }

    pub fn load_or_create_key(path: &std::path::Path) -> AppResult<[u8; KEY_LEN]> {
        if path.exists() {
            let metadata = std::fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(AppError::internal("master key path must be a regular file"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(AppError::internal(
                        "master key file permissions must not grant group or other access",
                    ));
                }
            }
            let mut bytes = std::fs::read(path)?;
            if bytes.len() != KEY_LEN {
                bytes.zeroize();
                return Err(AppError::internal("master key file has invalid length"));
            }
            let mut key = [0u8; KEY_LEN];
            key.copy_from_slice(&bytes);
            bytes.zeroize();
            Ok(key)
        } else {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
            let mut key = [0u8; KEY_LEN];
            rand::thread_rng().fill_bytes(&mut key);
            let result = create_master_key_file(path, &key);
            if let Err(error) = result {
                key.zeroize();
                return Err(error);
            }
            Ok(key)
        }
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(Key::from_slice(&self.master_key))
    }

    fn aad(secret_id: &str, owner_scope: &str, kind: &str, key_version: i64) -> Vec<u8> {
        format!("{secret_id}|{owner_scope}|{kind}|{key_version}").into_bytes()
    }

    fn fingerprint(plaintext: &[u8]) -> String {
        let digest = Sha256::digest(plaintext);
        format!("fp_{}", hex::encode(&digest[..6]))
    }
}

fn create_master_key_file(path: &std::path::Path, key: &[u8; KEY_LEN]) -> AppResult<()> {
    use std::io::Write;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| AppError::internal("master key path has no valid file name"))?;
    let temp_path = parent.join(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp_path)?;
    let write_result = file.write_all(key).and_then(|_| file.sync_all());
    drop(file);
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error.into());
    }
    if let Err(error) = std::fs::hard_link(&temp_path, path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error.into());
    }
    std::fs::remove_file(&temp_path)?;
    #[cfg(unix)]
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub fn derive_subkey(master_key: &[u8; KEY_LEN], context: &[u8]) -> AppResult<[u8; KEY_LEN]> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(master_key)
        .map_err(|_| AppError::internal("subkey derivation failed"))?;
    mac.update(context);
    let mut bytes = mac.finalize().into_bytes();
    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&bytes);
    bytes.zeroize();
    Ok(key)
}

impl Drop for EncryptedSqliteVault {
    fn drop(&mut self) {
        self.master_key.zeroize();
    }
}

#[async_trait]
impl SecretVault for EncryptedSqliteVault {
    async fn put(&self, owner_scope: &str, kind: &str, plaintext: &[u8]) -> AppResult<SecretMeta> {
        let id = new_id();
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let aad = Self::aad(&id, owner_scope, kind, self.key_version);
        let ciphertext = self
            .cipher()
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| AppError::internal("secret encryption failed"))?;
        let fingerprint = Self::fingerprint(plaintext);
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO secrets (id, owner_scope, kind, key_version, nonce, ciphertext, fingerprint, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(owner_scope)
        .bind(kind)
        .bind(self.key_version)
        .bind(&nonce_bytes[..])
        .bind(&ciphertext)
        .bind(&fingerprint)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(SecretMeta {
            id,
            kind: kind.to_string(),
            fingerprint,
            configured: true,
        })
    }

    async fn get(&self, secret_id: &str) -> AppResult<Vec<u8>> {
        let row = sqlx::query_as::<_, SecretRow>(
            "SELECT id, owner_scope, kind, key_version, nonce, ciphertext FROM secrets WHERE id = ?",
        )
        .bind(secret_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::not_found("SECRET_NOT_FOUND", "secret not found"))?;
        let nonce = XNonce::from_slice(&row.nonce);
        let aad = Self::aad(&row.id, &row.owner_scope, &row.kind, row.key_version);
        self.cipher()
            .decrypt(
                nonce,
                Payload {
                    msg: &row.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| AppError::internal("secret decryption failed"))
    }

    async fn delete(&self, secret_id: &str) -> AppResult<()> {
        sqlx::query("DELETE FROM secrets WHERE id = ?")
            .bind(secret_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn fingerprint(&self, secret_id: &str) -> AppResult<String> {
        let value: Option<(String,)> =
            sqlx::query_as("SELECT fingerprint FROM secrets WHERE id = ?")
                .bind(secret_id)
                .fetch_optional(&self.pool)
                .await?;
        value
            .map(|row| row.0)
            .ok_or_else(|| AppError::not_found("SECRET_NOT_FOUND", "secret not found"))
    }

    async fn rotate_master_key(&self, new_key: &[u8], dry_run: bool) -> AppResult<u32> {
        if !dry_run {
            return Err(AppError::service_unavailable(
                "MASTER_KEY_ROTATION_DISABLED",
                "master-key rotation requires the recoverable two-phase rotation protocol",
            ));
        }
        if new_key.len() != KEY_LEN {
            return Err(AppError::bad_request(
                "MASTER_KEY_INVALID",
                "new master key must be 32 bytes",
            ));
        }
        let rows = sqlx::query_as::<_, SecretRow>(
            "SELECT id, owner_scope, kind, key_version, nonce, ciphertext FROM secrets",
        )
        .fetch_all(&self.pool)
        .await?;
        let old_cipher = self.cipher();
        for row in &rows {
            let nonce = XNonce::from_slice(&row.nonce);
            let aad = Self::aad(&row.id, &row.owner_scope, &row.kind, row.key_version);
            let mut plaintext = old_cipher
                .decrypt(
                    nonce,
                    Payload {
                        msg: &row.ciphertext,
                        aad: &aad,
                    },
                )
                .map_err(|_| {
                    AppError::internal("secret decryption failed during rotation check")
                })?;
            plaintext.zeroize();
        }
        u32::try_from(rows.len()).map_err(|_| AppError::internal("secret count exceeds u32"))
    }
}

#[derive(sqlx::FromRow)]
struct SecretRow {
    id: String,
    owner_scope: String,
    kind: String,
    key_version: i64,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
}

pub fn bind_code_hmac(master_key: &[u8], code: &str) -> AppResult<String> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(master_key)
        .map_err(|_| AppError::internal("hmac key invalid"))?;
    mac.update(code.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn hmac_eq(left: &str, right: &str) -> bool {
    use subtle::ConstantTimeEq;
    left.as_bytes().ct_eq(right.as_bytes()).into()
}
