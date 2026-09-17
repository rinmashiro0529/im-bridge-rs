use im_bridge::modules::bridge::{operation_payload, st_ops};

type LocatorHash = fn(&str, &str, &str) -> String;

// These assignments also preserve the two original callable public paths.
const HASHERS: [LocatorHash; 2] = [st_ops::locator_hash, operation_payload::locator_hash];

fn assert_vector(parts: [&str; 3], expected: &str) {
    for hash in HASHERS {
        assert_eq!(hash(parts[0], parts[1], parts[2]), expected);
    }
}

// Fixed expectations were produced independently with Python hashlib.sha256
// and struct.pack(">Q", len(field.encode("utf-8"))). Do not generate expected
// values by calling either Rust implementation under test.
#[test]
fn public_paths_match_fixed_vectors() {
    for (parts, expected) in [
        (
            ["", "", ""],
            "9d908ecfb6b256def8b49a7c504e6c889c4b0e41fe6ce3e01863dd7b61a20aa0",
        ),
        (
            ["a", "bc", ""],
            "74f2dff90c16bd75e74ea8ab93e0f683754b4ae5a7e2b6c960734cf5c656341a",
        ),
        (
            ["ab", "c", ""],
            "528e931465bb50b57f335f8f47b78a4e3a2ee8b14454d7d3586105668d33c410",
        ),
        (
            ["a:b", "c", "d"],
            "e7dfc6ef14022ec462bc7c798dee2e8de3ebb7be2022acaad5441b764ed1511b",
        ),
        (
            ["a", "b:c", "d"],
            "f9069e9c8bb0bf641329e4c25194c20117d742bde74c6d5ebe08450f291cbddb",
        ),
        (
            [
                "\u{7528}\u{6237}",
                "\u{89d2}\u{8272}\u{1f600}.png",
                "\u{4f1a}\u{8bdd}.jsonl",
            ],
            "d9973b32cba628c6f19319c7c3d31991e7915a6a5aa1f7306bc4bc6cfc4adf11",
        ),
        (
            ["\0", "a\0b", "line\nbreak"],
            "54f2ec6bc6706af8f47f1178ca855d93dd4d4a4a1890746603da3c018c604b15",
        ),
        (
            ["default-user", "Synthetic.png", "history.jsonl"],
            "27285630a1a614ccd6ba45780921f5235f593b4df083c763dee8a52bd1174498",
        ),
    ] {
        assert_vector(parts, expected);
    }
}

#[test]
fn unicode_normalization_is_not_introduced() {
    assert_vector(
        ["\u{00e9}", "avatar", "chat"],
        "ac0640f3029cbcb8cdc457eb7152e680db2ef290f68279d8c621c7432696d413",
    );
    assert_vector(
        ["e\u{0301}", "avatar", "chat"],
        "7b7ca5686c44040cd0d7623c5da33a330c6622c9a0cebd76a23bb9dc71342440",
    );
}

#[test]
fn long_field_lengths_are_not_truncated() {
    let handle = "x".repeat(4096);
    let avatar = "\u{1f600}".repeat(256);
    let chat = "y".repeat(65536);
    assert_vector(
        [&handle, &avatar, &chat],
        "473b06c8ef6243a0f31ebb19f945f02fcbcbd5311da4dd2ebe5f3cabd2e632f9",
    );
}
