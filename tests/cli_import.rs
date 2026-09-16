use std::process::Command;

#[test]
fn import_dry_run_does_not_touch_configured_database_or_master_key() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("st-data");
    std::fs::create_dir_all(&source).unwrap();
    let target = dir.path().join("target-data");
    let database = target.join("app.db");
    let master_key = dir.path().join("target-master.key");
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::json!({
            "listen": "127.0.0.1:0",
            "data_dir": target,
            "database_path": database,
            "master_key_path": master_key,
            "session_ttl_hours": 12,
            "cookie_secure": false
        })
        .to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_im-bridge"))
        .args([
            "import-st",
            "--data-root",
            source.to_str().unwrap(),
            "--dry-run",
            "--config",
            config.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!database.exists());
    assert!(!master_key.exists());
}
