use im_bridge::modules::bridge::connector_hmac::{
    body_sha256, canonical_string, normalize_query, sign, verify, ConnectorHmacError,
    ConnectorHmacRequest, MemoryNonceSet, ALLOWED_CONTENT_TYPE, MAX_SKEW_SECS,
};

const SYNTHETIC_KEY: &[u8] = b"synthetic-connector-hmac-key-32b!";
const BODY: &[u8] = br#"{"fixture":true}"#;
const COMMIT_PATH: &str = "/api/plugins/st-im-bridge/connector/v1/chats/commit";
const NOW: i64 = 1_710_000_000;

fn request<'a>(
    path: &'a str,
    query: &'a str,
    timestamp_unix: i64,
    nonce: &'a str,
    content_type: &'a str,
    body: &'a [u8],
) -> ConnectorHmacRequest<'a> {
    ConnectorHmacRequest {
        method: "POST",
        path,
        query,
        timestamp_unix,
        nonce,
        content_type,
        body,
    }
}

fn signed(req: &ConnectorHmacRequest<'_>) -> Result<(String, String), ConnectorHmacError> {
    let canonical = canonical_string(req)?;
    let hex = sign(SYNTHETIC_KEY, &canonical)?;
    Ok((canonical, hex))
}

#[test]
fn canonical_string_is_stable_for_the_locked_fixture() {
    let req = request(
        COMMIT_PATH,
        "b=2&a=1",
        NOW,
        "nOnce_1",
        ALLOWED_CONTENT_TYPE,
        BODY,
    );
    assert_eq!(normalize_query(req.query).unwrap(), "a=1&b=2");
    assert_eq!(
        body_sha256(BODY),
        "ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2"
    );
    let canonical = canonical_string(&req).expect("canonical fixture");
    assert_eq!(
        canonical,
        "POST\n/api/plugins/st-im-bridge/connector/v1/chats/commit\na=1&b=2\n1710000000\nnOnce_1\napplication/octet-stream\nffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2"
    );
}

#[test]
fn duplicate_query_keys_are_ambiguous() {
    assert_eq!(
        normalize_query("a=1&a=2"),
        Err(ConnectorHmacError::AmbiguousQuery)
    );
    let req = request(
        COMMIT_PATH,
        "a=1&a=2",
        NOW,
        "nOnce_01",
        ALLOWED_CONTENT_TYPE,
        BODY,
    );
    assert_eq!(
        canonical_string(&req),
        Err(ConnectorHmacError::AmbiguousQuery)
    );
}

#[test]
fn json_content_type_is_rejected_even_with_a_matching_signature() {
    let valid = request(COMMIT_PATH, "", NOW, "nOnce_01", ALLOWED_CONTENT_TYPE, BODY);
    let signature = signed(&valid).expect("sign octet-stream").1;
    let json = request(COMMIT_PATH, "", NOW, "nOnce_01", "application/json", BODY);
    let mut seen = MemoryNonceSet::default();
    let error = verify(SYNTHETIC_KEY, &json, &signature, NOW, &mut seen)
        .expect_err("json media type must fail closed");
    assert_eq!(error, ConnectorHmacError::MediaType);
}

#[test]
fn timestamp_skew_is_inclusive_at_thirty_seconds() {
    let mut seen = MemoryNonceSet::default();
    for (nonce, timestamp) in [
        ("nOnce_30a", NOW + MAX_SKEW_SECS),
        ("nOnce_30b", NOW - MAX_SKEW_SECS),
    ] {
        let req = request(
            COMMIT_PATH,
            "",
            timestamp,
            nonce,
            ALLOWED_CONTENT_TYPE,
            BODY,
        );
        let signature = signed(&req).expect("sign in-window").1;
        verify(SYNTHETIC_KEY, &req, &signature, NOW, &mut seen).expect("±30 must succeed");
    }

    for (nonce, timestamp) in [("nOnce_31a", NOW + 31), ("nOnce_31b", NOW - 31)] {
        let req = request(
            COMMIT_PATH,
            "",
            timestamp,
            nonce,
            ALLOWED_CONTENT_TYPE,
            BODY,
        );
        let signature = signed(&req).expect("sign out-of-window").1;
        let error = verify(SYNTHETIC_KEY, &req, &signature, NOW, &mut seen)
            .expect_err("±31 must be rejected");
        assert_eq!(error, ConnectorHmacError::TimestampSkew);
    }
}

#[test]
fn nonce_replay_is_rejected_even_with_a_new_timestamp() {
    let first = request(COMMIT_PATH, "", NOW, "nOnce_rp", ALLOWED_CONTENT_TYPE, BODY);
    let signature = signed(&first).expect("sign first nonce").1;
    let mut seen = MemoryNonceSet::default();
    verify(SYNTHETIC_KEY, &first, &signature, NOW, &mut seen).expect("first nonce accepted");

    let replay = request(
        COMMIT_PATH,
        "",
        NOW + 1,
        "nOnce_rp",
        ALLOWED_CONTENT_TYPE,
        BODY,
    );
    let replay_signature = signed(&replay).expect("sign replay").1;
    let error = verify(
        SYNTHETIC_KEY,
        &replay,
        &replay_signature,
        NOW + 1,
        &mut seen,
    )
    .expect_err("same nonce is replay");
    assert_eq!(error, ConnectorHmacError::NonceReplay);
}

#[test]
fn wrong_signature_is_mismatch() {
    let req = request(COMMIT_PATH, "", NOW, "nOnce_sg", ALLOWED_CONTENT_TYPE, BODY);
    let mut signature = signed(&req).expect("sign valid").1;
    let last = signature.pop().expect("hex char");
    signature.push(if last == '0' { '1' } else { '0' });
    let mut seen = MemoryNonceSet::default();
    let error = verify(SYNTHETIC_KEY, &req, &signature, NOW, &mut seen)
        .expect_err("flipped hex must mismatch");
    assert_eq!(error, ConnectorHmacError::SignatureMismatch);
}

#[test]
fn path_traversal_and_double_slash_are_invalid() {
    for path in ["../chats", "//x"] {
        let req = request(path, "", NOW, "nOnce_pt", ALLOWED_CONTENT_TYPE, BODY);
        assert_eq!(canonical_string(&req), Err(ConnectorHmacError::InvalidPath));
        let mut seen = MemoryNonceSet::default();
        let error = verify(SYNTHETIC_KEY, &req, "00", NOW, &mut seen)
            .expect_err("illegal path must fail before signature");
        assert_eq!(error, ConnectorHmacError::InvalidPath);
    }
}

#[test]
fn display_and_errors_omit_key_and_body() {
    let req = request(COMMIT_PATH, "", NOW, "nOnce_ds", ALLOWED_CONTENT_TYPE, BODY);
    let mut seen = MemoryNonceSet::default();
    let error =
        verify(SYNTHETIC_KEY, &req, "deadbeef", NOW, &mut seen).expect_err("short hex mismatches");
    let rendered = format!("{error} {error:?}");
    let key_text = std::str::from_utf8(SYNTHETIC_KEY).expect("ascii key");
    assert!(!rendered.contains(key_text), "display leaked key");
    assert!(
        !rendered.contains(r#"{"fixture":true}"#),
        "display leaked body"
    );
    for variant in [
        ConnectorHmacError::AmbiguousQuery,
        ConnectorHmacError::InvalidPath,
        ConnectorHmacError::MediaType,
        ConnectorHmacError::TimestampSkew,
        ConnectorHmacError::NonceReplay,
        ConnectorHmacError::InvalidNonce,
        ConnectorHmacError::InvalidKey,
        ConnectorHmacError::SignatureMismatch,
    ] {
        let text = variant.to_string();
        assert!(!text.contains(key_text));
        assert!(!text.contains(r#"{"fixture":true}"#));
        assert!(!text.contains("synthetic-connector-hmac-key"));
    }
}
