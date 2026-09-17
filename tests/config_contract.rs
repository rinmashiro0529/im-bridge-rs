use std::path::Path;
use std::process::{Command, Output};

use im_bridge::config::{AppConfig, StClientConfig};
use serde_json::{json, Value};

// Independent contract values, intentionally not imported from implementation.
const TIMEOUTS: [(&str, &str, u64, u64); 3] = [
    ("timeout_ms", "IMBRIDGE_ST_TIMEOUT_MS", 100, 120_000),
    (
        "generate_hard_timeout_ms",
        "IMBRIDGE_ST_GENERATE_HARD_TIMEOUT_MS",
        1_000,
        3_600_000,
    ),
    (
        "generate_idle_timeout_ms",
        "IMBRIDGE_ST_GENERATE_IDLE_TIMEOUT_MS",
        1_000,
        600_000,
    ),
];

fn document(dir: &Path, st: Value) -> Value {
    json!({
        "listen": "127.0.0.1:0",
        "data_dir": dir.join("data"),
        "database_path": dir.join("data/app.db"),
        "master_key_path": dir.join("master.key"),
        "session_ttl_hours": 12,
        "cookie_secure": false,
        "st": st
    })
}

fn write_config(dir: &Path, value: &Value) -> std::path::PathBuf {
    let path = dir.join("config.json");
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    path
}

fn doctor(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_im-bridge"));
    // Test the real environment/CLI path without racing process-global env.
    command.env_clear();
    for name in ["PATH", "SystemRoot", "WINDIR"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
        .current_dir(dir)
        .arg("doctor")
        .env("IMBRIDGE_DATA_DIR", dir.join("data"))
        .env("IMBRIDGE_MASTER_KEY_PATH", dir.join("master.key"))
        .env("IMBRIDGE_ST_MODE", "disabled");
    command
}

fn assert_invalid_without_side_effects(dir: &Path, output: &Output) {
    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("CONFIG_INVALID"));
    assert!(!dir.join("data").exists());
    assert!(!dir.join("master.key").exists());
}

#[test]
fn omitted_or_partial_st_configuration_defaults_to_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let mut omitted = document(dir.path(), json!({}));
    omitted.as_object_mut().unwrap().remove("st");
    for value in [
        omitted,
        document(dir.path(), json!({})),
        document(dir.path(), json!({"handle": "synthetic-user"})),
    ] {
        let config: AppConfig = serde_json::from_value(value).unwrap();
        config.validate().unwrap();
        assert_eq!(config.st.mode, "disabled");
        assert_eq!(config.st.timeout_ms, 15_000);
        assert_eq!(config.st.generate_hard_timeout_ms, 900_000);
        assert_eq!(config.st.generate_idle_timeout_ms, 90_000);
        assert!(config.st.connector_hmac_key.is_none());
    }
    let explicit_empty: StClientConfig = serde_json::from_value(json!({"mode": ""})).unwrap();
    assert_eq!(
        explicit_empty.validate().unwrap_err().code,
        "CONFIG_INVALID"
    );
}

#[test]
fn final_validation_checks_directly_constructed_values_as_well_as_json() {
    for (field, env, min, max) in TIMEOUTS {
        for value in [0, min - 1, min, max, max + 1, u64::MAX] {
            let mut json_config = json!({});
            json_config[field] = json!(value);
            let from_json: StClientConfig = serde_json::from_value(json_config).unwrap();
            let mut direct = StClientConfig::default();
            match field {
                "timeout_ms" => direct.timeout_ms = value,
                "generate_hard_timeout_ms" => direct.generate_hard_timeout_ms = value,
                "generate_idle_timeout_ms" => direct.generate_idle_timeout_ms = value,
                _ => unreachable!(),
            }
            for config in [direct, from_json] {
                let result = config.validate();
                assert_eq!(
                    result.is_ok(),
                    (min..=max).contains(&value),
                    "{field}/{value}"
                );
                if let Err(error) = result {
                    assert_eq!(error.code, "CONFIG_INVALID");
                    assert_eq!(
                        error.message,
                        format!("{env} must be between {min} and {max}")
                    );
                }
            }
        }
    }
}

#[test]
fn actual_cli_enforces_identical_timeout_boundaries_for_environment_and_file() {
    for (field, env, min, max) in TIMEOUTS {
        for value in [0, min - 1, min, max, max + 1, u64::MAX] {
            for file_source in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut command = doctor(dir.path());
                if file_source {
                    let mut st = json!({});
                    st[field] = json!(value);
                    let path = write_config(dir.path(), &document(dir.path(), st));
                    command.arg("--config").arg(path);
                } else {
                    command.env(env, value.to_string());
                }
                let output = command.output().unwrap();
                if (min..=max).contains(&value) {
                    assert!(
                        output.status.success(),
                        "{field}/{value}/{file_source}: {output:?}"
                    );
                    assert!(String::from_utf8_lossy(&output.stdout).contains("database=ok"));
                } else {
                    assert_invalid_without_side_effects(dir.path(), &output);
                    assert!(String::from_utf8_lossy(&output.stderr)
                        .contains(&format!("{env} must be between {min} and {max}")));
                }
            }
        }
    }
}

#[test]
fn session_ttl_keeps_signed_parsing_and_inclusive_limits() {
    for value in [i64::MIN, -1, 0, 1, 168, 169, i64::MAX] {
        for file_source in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut command = doctor(dir.path());
            if file_source {
                let mut config = document(dir.path(), json!({}));
                config["session_ttl_hours"] = json!(value);
                command
                    .arg("--config")
                    .arg(write_config(dir.path(), &config));
            } else {
                command.env("IMBRIDGE_SESSION_TTL_HOURS", value.to_string());
            }
            let output = command.output().unwrap();
            if (1..=168).contains(&value) {
                assert!(output.status.success(), "{value}/{file_source}: {output:?}");
            } else {
                assert_invalid_without_side_effects(dir.path(), &output);
            }
        }
    }
}

#[test]
fn malformed_environment_numbers_fail_without_echoing_the_input() {
    for raw in [
        "",
        "-1",
        "1.5",
        " 100",
        "synthetic-sensitive-value",
        "18446744073709551616",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let output = doctor(dir.path())
            .env("IMBRIDGE_ST_TIMEOUT_MS", raw)
            .output()
            .unwrap();
        assert_invalid_without_side_effects(dir.path(), &output);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-sensitive-value"));
    }
}

#[cfg(unix)]
#[test]
fn non_utf8_environment_numbers_have_a_stable_configuration_error() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let output = doctor(dir.path())
        .env(
            "IMBRIDGE_ST_TIMEOUT_MS",
            std::ffi::OsString::from_vec(vec![0xff]),
        )
        .output()
        .unwrap();
    assert_invalid_without_side_effects(dir.path(), &output);
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("IMBRIDGE_ST_TIMEOUT_MS is not valid UTF-8"));
}

#[test]
fn explicit_file_remains_authoritative_over_appconfig_environment_variables() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), &document(dir.path(), json!({})));
    let output = doctor(dir.path())
        .arg("--config")
        .arg(path)
        .env("IMBRIDGE_LISTEN", "not-a-socket")
        .env("IMBRIDGE_SESSION_TTL_HOURS", "0")
        .env("IMBRIDGE_COOKIE_SECURE", "not-a-bool")
        .env("IMBRIDGE_ST_TIMEOUT_MS", "0")
        .env("IMBRIDGE_ST_MODE", "production_write")
        .env("IMBRIDGE_ST_CONNECTOR_HMAC_KEY", "synthetic-weak-key")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn existing_security_and_unknown_field_guards_are_not_relaxed() {
    let dir = tempfile::tempdir().unwrap();
    for st in [
        json!({"mode": "unknown"}),
        json!({"mode": "production_write", "connector_hmac_key": "synthetic-weak-key"}),
        json!({"handle": ""}),
        json!({"base_url": "http://service.example"}),
        json!({"base_url": "https://user:password@service.example"}),
        json!({"base_url": "https://service.example?secret=synthetic"}),
        json!({"base_url": "https://service.example#fragment"}),
    ] {
        let config: AppConfig = serde_json::from_value(document(dir.path(), st)).unwrap();
        assert_eq!(config.validate().unwrap_err().code, "CONFIG_INVALID");
    }
    assert!(
        serde_json::from_value::<AppConfig>(document(dir.path(), json!({"typo": true}))).is_err()
    );
    let mut unknown = document(dir.path(), json!({}));
    unknown["typo"] = json!(true);
    assert!(serde_json::from_value::<AppConfig>(unknown).is_err());
    let mut public_plain = document(dir.path(), json!({}));
    public_plain["listen"] = json!("0.0.0.0:8787");
    let config: AppConfig = serde_json::from_value(public_plain).unwrap();
    assert_eq!(config.validate().unwrap_err().code, "CONFIG_INVALID");
}
