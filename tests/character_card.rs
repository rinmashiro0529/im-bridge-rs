use im_bridge::domain::character::NormalizedCardFields;
use im_bridge::modules::characters::card_png::{
    encode_png_with_card, parse_character_bytes, parse_json_card,
};

fn minimal_png() -> Vec<u8> {
    let mut png = vec![137, 80, 78, 71, 13, 10, 26, 10];
    let ihdr_data = {
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&[8, 2, 0, 0, 0]);
        data
    };
    write_chunk(&mut png, b"IHDR", &ihdr_data);
    write_chunk(
        &mut png,
        b"IDAT",
        &[
            0x08, 0xd7, 0x63, 0xf8, 0xcf, 0xc0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
        ],
    );
    write_chunk(&mut png, b"IEND", &[]);
    png
}

fn write_chunk(out: &mut Vec<u8>, ty: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(ty);
    out.extend_from_slice(data);
    let mut crc_input = Vec::from(*ty);
    crc_input.extend_from_slice(data);
    out.extend_from_slice(&crc32fast::hash(&crc_input).to_be_bytes());
}

#[test]
fn json_v2_round_trip_keeps_unknown_fields() {
    let bytes = std::fs::read("fixtures/character_cards/v2.json").unwrap();
    let parsed = parse_json_card(&bytes, "json").unwrap();
    assert_eq!(parsed.spec.as_deref(), Some("chara_card_v2"));
    let normalized = NormalizedCardFields::from_raw(&parsed.raw);
    assert_eq!(normalized.name, "TestChar");
    assert_eq!(
        parsed.raw["data"]["extensions"]["custom_unknown"]["keep"],
        true
    );
    assert_eq!(
        parsed.raw["data"]["creator_notes"],
        "keep this unknown-to-bridge field"
    );
}

#[test]
fn png_prefers_ccv3_and_round_trips() {
    let raw = serde_json::from_slice::<serde_json::Value>(
        &std::fs::read("fixtures/character_cards/v2.json").unwrap(),
    )
    .unwrap();
    let png = encode_png_with_card(&minimal_png(), &raw).unwrap();
    let parsed = parse_character_bytes(&png, "card.png").unwrap();
    assert_eq!(parsed.source_format, "png");
    assert_eq!(parsed.raw["data"]["name"], "TestChar");
    assert_eq!(
        parsed.raw["data"]["extensions"]["custom_unknown"]["keep"],
        true
    );
}
