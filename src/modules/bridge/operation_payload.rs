use std::sync::Arc;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use zeroize::{Zeroize, Zeroizing};

use crate::error::{AppError, AppResult};
use crate::modules::bridge::operation_store::OperationStore;
use crate::modules::bridge::operations::{EncryptedOperationPayload, OperationAad};
use crate::seams::secret_vault::SecretVault;

pub use crate::domain::locator::locator_hash;

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const OPERATION_DEK_KIND: &str = "operation_dek_v1";

struct OperationDek(Zeroizing<[u8; KEY_LEN]>);

impl OperationDek {
    fn random() -> Self {
        let mut bytes = [0u8; KEY_LEN];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(Zeroizing::new(bytes))
    }

    fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

impl Clone for OperationDek {
    fn clone(&self) -> Self {
        Self::from_bytes(*self.as_bytes())
    }
}

impl Drop for OperationDek {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Encrypts the minimal typed operation payload with an operation-scoped DEK.
///
/// `new` remains available for deterministic component tests and callers that
/// already obtained a DEK from an approved key registry.  Runtime bootstrap
/// should use `random` or `create_wrapped`, never the master database key.
pub struct OperationPayloadEncryptor {
    operation_dek: OperationDek,
    key_version: u32,
    wrapped_secret_id: Option<String>,
}

impl OperationPayloadEncryptor {
    pub fn new(operation_dek: [u8; KEY_LEN], key_version: u32) -> Self {
        Self {
            operation_dek: OperationDek::from_bytes(operation_dek),
            key_version,
            wrapped_secret_id: None,
        }
    }

    pub fn random(key_version: u32) -> Self {
        Self {
            operation_dek: OperationDek::random(),
            key_version,
            wrapped_secret_id: None,
        }
    }

    pub fn new_random(key_version: u32) -> Self {
        Self::random(key_version)
    }

    /// Create one independent DEK and store only its wrapped form in the
    /// configured vault.  The returned encryptor retains the zeroizing DEK for
    /// the current operation; the master key never enters this type.
    pub async fn create_wrapped(
        vault: Arc<dyn SecretVault>,
        operation_id: &str,
        key_version: u32,
    ) -> AppResult<Self> {
        if operation_id.trim().is_empty() {
            return Err(AppError::bad_request(
                "PAYLOAD_OPERATION_ID_REQUIRED",
                "operation id is required for operation DEK",
            ));
        }
        let operation_dek = OperationDek::random();
        let secret = vault
            .put(
                &format!("operation:{operation_id}"),
                OPERATION_DEK_KIND,
                operation_dek.as_bytes(),
            )
            .await?;
        Ok(Self {
            operation_dek,
            key_version,
            wrapped_secret_id: Some(secret.id),
        })
    }

    pub async fn from_wrapped(
        vault: Arc<dyn SecretVault>,
        secret_id: &str,
        key_version: u32,
    ) -> AppResult<Self> {
        if secret_id.trim().is_empty() {
            return Err(AppError::bad_request(
                "PAYLOAD_KEY_REQUIRED",
                "wrapped operation DEK id is required",
            ));
        }
        let mut bytes = vault.get(secret_id).await?;
        if bytes.len() != KEY_LEN {
            bytes.zeroize();
            return Err(decrypt_failed());
        }
        let mut operation_dek = [0u8; KEY_LEN];
        operation_dek.copy_from_slice(&bytes);
        bytes.zeroize();
        Ok(Self {
            operation_dek: OperationDek::from_bytes(operation_dek),
            key_version,
            wrapped_secret_id: Some(secret_id.to_string()),
        })
    }

    pub fn key_version(&self) -> u32 {
        self.key_version
    }

    pub fn wrapped_secret_id(&self) -> Option<&str> {
        self.wrapped_secret_id.as_deref()
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(Key::from_slice(self.operation_dek.as_bytes()))
    }

    pub fn encrypt(
        &self,
        plaintext: &[u8],
        aad: &OperationAad,
    ) -> AppResult<EncryptedOperationPayload> {
        validate_aad(aad)?;
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let aad_bytes = aad.encode();
        let ciphertext = self
            .cipher()
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad_bytes,
                },
            )
            .map_err(|_| {
                AppError::new(
                    "PAYLOAD_ENCRYPT_FAILED",
                    "operation payload encryption failed",
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                )
            })?;
        Ok(EncryptedOperationPayload {
            ciphertext,
            nonce: nonce_bytes,
            key_version: self.key_version,
        })
    }

    pub fn decrypt(
        &self,
        encrypted: &EncryptedOperationPayload,
        aad: &OperationAad,
    ) -> AppResult<Vec<u8>> {
        if encrypted.key_version != self.key_version {
            return Err(decrypt_failed());
        }
        validate_aad(aad).map_err(|_| decrypt_failed())?;
        let nonce = XNonce::from_slice(&encrypted.nonce);
        let aad_bytes = aad.encode();
        self.cipher()
            .decrypt(
                nonce,
                Payload {
                    msg: &encrypted.ciphertext,
                    aad: &aad_bytes,
                },
            )
            .map_err(|_| decrypt_failed())
    }
}

#[derive(Clone)]
pub struct OperationPayloadKeyProvider {
    vault: Arc<dyn SecretVault>,
    store: Arc<OperationStore>,
    key_version: u32,
}

impl OperationPayloadKeyProvider {
    pub fn new(vault: Arc<dyn SecretVault>, store: Arc<OperationStore>) -> Self {
        Self::with_key_version(vault, store, 1)
    }

    pub fn with_key_version(
        vault: Arc<dyn SecretVault>,
        store: Arc<OperationStore>,
        key_version: u32,
    ) -> Self {
        Self {
            vault,
            store,
            key_version: key_version.max(1),
        }
    }

    pub async fn for_operation(
        &self,
        operation_id: &str,
    ) -> AppResult<Arc<OperationPayloadEncryptor>> {
        if let Some(reference) = self.store.get_operation_key_reference(operation_id).await? {
            return Ok(Arc::new(
                OperationPayloadEncryptor::from_wrapped(
                    self.vault.clone(),
                    &reference.wrapped_secret_id,
                    reference.key_version,
                )
                .await?,
            ));
        }
        let encryptor = OperationPayloadEncryptor::create_wrapped(
            self.vault.clone(),
            operation_id,
            self.key_version,
        )
        .await?;
        let secret_id = encryptor
            .wrapped_secret_id()
            .ok_or_else(|| AppError::internal("operation DEK reference was not returned"))?
            .to_string();
        match self
            .store
            .put_operation_key_reference(operation_id, &secret_id, encryptor.key_version())
            .await
        {
            Ok(()) => Ok(Arc::new(encryptor)),
            Err(error) => {
                if let Some(reference) =
                    self.store.get_operation_key_reference(operation_id).await?
                {
                    return Ok(Arc::new(
                        OperationPayloadEncryptor::from_wrapped(
                            self.vault.clone(),
                            &reference.wrapped_secret_id,
                            reference.key_version,
                        )
                        .await?,
                    ));
                }
                Err(error)
            }
        }
    }

    pub async fn restore_for_operation(
        &self,
        operation_id: &str,
    ) -> AppResult<Arc<OperationPayloadEncryptor>> {
        let reference = self
            .store
            .get_operation_key_reference(operation_id)
            .await?
            .ok_or_else(|| {
                AppError::service_unavailable(
                    "PAYLOAD_KEY_REFERENCE_MISSING",
                    "operation payload key reference is missing",
                )
            })?;
        Ok(Arc::new(
            OperationPayloadEncryptor::from_wrapped(
                self.vault.clone(),
                &reference.wrapped_secret_id,
                reference.key_version,
            )
            .await?,
        ))
    }
}

fn validate_aad(aad: &OperationAad) -> AppResult<()> {
    if aad.version == 0
        || aad.operation_id.trim().is_empty()
        || aad.locator_hash.trim().is_empty()
        || aad.operation_kind.trim().is_empty()
        || aad.actor_id.trim().is_empty()
        || aad.bot_id.trim().is_empty()
    {
        return Err(AppError::bad_request(
            "PAYLOAD_AAD_INVALID",
            "operation payload authenticated identity is incomplete",
        ));
    }
    Ok(())
}

fn decrypt_failed() -> AppError {
    AppError::new(
        "PAYLOAD_DECRYPT_FAILED",
        "operation payload could not be decrypted",
        axum::http::StatusCode::BAD_REQUEST,
    )
}
