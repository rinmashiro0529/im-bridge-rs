use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::adapters::sqlite::{connect_pool, migrate};
use im_bridge::seams::secret_vault::SecretVault;

#[tokio::test]
async fn secrets_are_encrypted_and_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let pool = connect_pool(&dir.path().join("app.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let mut key = [7u8; 32];
    let vault = EncryptedSqliteVault::new(pool.clone(), key);
    let meta = vault
        .put("ws", "telegram_bot_token", b"123456:secret-token")
        .await
        .unwrap();
    let blob: (Vec<u8>,) = sqlx::query_as("SELECT ciphertext FROM secrets WHERE id = ?")
        .bind(&meta.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let haystack = String::from_utf8_lossy(&blob.0);
    assert!(!haystack.contains("secret-token"));
    assert_eq!(vault.get(&meta.id).await.unwrap(), b"123456:secret-token");
    let mut new_key = [9u8; 32];
    let count = vault.rotate_master_key(&new_key, true).await.unwrap();
    assert_eq!(count, 1);
    key = [0u8; 32];
    new_key = [0u8; 32];
    let _ = (key, new_key);
}
