use im_bridge::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use im_bridge::modules::bridge::redaction::redact_detail;

#[test]
fn sensitive_values_are_removed_across_supported_forms() {
    let cases: &[(&str, &str, &[&str])] = &[
        (
            "header values",
            "Authorization: Bearer bearer-credential Authorization: Basic basic-credential x-csrf-token: header-csrf bot_token=header-bot api_key=header-api API Key: header-api-spaced Cookie: header-cookie",
            &[
                "Bearer bearer-credential",
                "bearer-credential",
                "Basic basic-credential",
                "basic-credential",
                "header-cookie",
                "header-csrf",
                "header-bot",
                "header-api",
                "header-api-spaced",
            ],
        ),
        (
            "JSON values",
            r#"{"authorization":"Bearer json-bearer","authorization":"Basic json-basic","cookie":"json-cookie","x-csrf-token":"json-csrf","bot_token":"json-bot","api_key":"json-key"}"#,
            &[
                "Bearer json-bearer",
                "json-bearer",
                "Basic json-basic",
                "json-basic",
                "json-cookie",
                "json-csrf",
                "json-bot",
                "json-key",
            ],
        ),
        (
            "URL userinfo and query values",
            "https://user:password@example.test/path?secret=query-secret&token=query-token&api_key=query-api-key&access_token=query-access-token",
            &[
                "user:password",
                "query-secret",
                "query-token",
                "query-api-key",
                "query-access-token",
            ],
        ),
        (
            "absolute paths",
            "C:\\Users\\Synthetic\\private.db \\\\server\\share\\private.db /home/private.db /srv/app/config /var/lib/private.db /users/synthetic/private.db /private/secret.db /root/private.db /opt/app/config /etc/secret.conf",
            &[
                "C:\\Users\\Synthetic\\private.db",
                "\\\\server\\share\\private.db",
                "/home/private.db",
                "/srv/app/config",
                "/var/lib/private.db",
                "/users/synthetic/private.db",
                "/private/secret.db",
                "/root/private.db",
                "/opt/app/config",
                "/etc/secret.conf",
            ],
        ),
        (
            "long token-like value",
            "syntheticlongtoken012345678901234567890123",
            &["syntheticlongtoken012345678901234567890123"],
        ),
    ];

    for &(label, input, secrets) in cases {
        let detail = redact_detail(input).expect("non-empty safe detail");
        for &secret in secrets {
            assert!(
                !detail.contains(secret),
                "{label}: secret remained: {secret}"
            );
        }
    }
}

#[test]
fn unquoted_cookie_header_redacts_all_cookie_pairs() {
    let detail = redact_detail("Cookie: sid=first-secret; prefs=second-secret\nstatus=ok")
        .expect("safe detail");
    assert!(!detail.contains("first-secret"));
    assert!(!detail.contains("second-secret"));
    assert!(detail.contains("status=ok"));
}

#[test]
fn url_userinfo_is_redacted_with_or_without_password_separator() {
    let detail =
        redact_detail("https://token-user@example.test/path https://user:pass@example.test/path")
            .expect("safe detail");
    assert!(!detail.contains("token-user"));
    assert!(!detail.contains("user:pass"));
    assert!(detail.contains("example.test/path"));
}

#[test]
fn ordinary_relative_paths_and_slashes_are_preserved() {
    let detail = redact_detail("relative/path /api/v1 plain/slash").expect("safe detail");
    assert!(detail.contains("relative/path"));
    assert!(detail.contains("/api/v1"));
    assert!(detail.contains("plain/slash"));
}

#[test]
fn unsafe_or_empty_detail_is_not_persisted_as_raw_text() {
    assert!(redact_detail("").is_none());
    assert!(redact_detail(r#"{"prompt":"private"}"#).is_none());
    assert!(redact_detail(r#"{"chat_metadata":"private"}"#).is_none());

    let error = StBridgeError::new(
        StErrorCode::StGenerateStateUnknown,
        StErrorStage::Generation,
        "synthetic safe message",
        false,
        CommitState::Unknown,
    )
    .with_safe_detail("");
    assert!(error.safe_detail.is_none());
}

#[test]
fn detail_length_is_bounded_after_redaction() {
    let detail = redact_detail(&"ordinary ".repeat(100)).expect("safe detail");
    assert!(detail.chars().count() <= 300);
}
