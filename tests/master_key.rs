use im_bridge::bootstrap::AppState;
use im_bridge::config::AppConfig;

#[tokio::test]
async fn bootstrap_rejects_a_wrong_master_key() {
    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: im_bridge::config::StClientConfig::default(),
    };
    let state = AppState::bootstrap(config.clone(), true).await.unwrap();
    drop(state);
    std::fs::write(&config.master_key_path, [9u8; 32]).unwrap();
    assert!(AppState::bootstrap(config, true).await.is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn master_key_rejects_group_or_other_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("master.key");
    std::fs::write(&key_path, [7_u8; 32]).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let result =
        im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault::load_or_create_key(
            &key_path,
        );
    assert!(result.is_err());
}
