use std::io::{Cursor, Read};

use base64::Engine;
use serde_json::Value;

use crate::error::{AppError, AppResult};

const PNG_SIG: &[u8] = &[137, 80, 78, 71, 13, 10, 26, 10];
const MAX_PNG_BYTES: usize = 16 * 1024 * 1024;
const MAX_JSON_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ParsedCard {
    pub source_format: String,
    pub spec: Option<String>,
    pub spec_version: Option<String>,
    pub raw: Value,
    pub checksum: String,
    pub warnings: Vec<String>,
}

pub fn parse_character_bytes(bytes: &[u8], filename: &str) -> AppResult<ParsedCard> {
    if bytes.len() > MAX_PNG_BYTES {
        return Err(AppError::bad_request(
            "CARD_TOO_LARGE",
            "character file exceeds size limit",
        ));
    }
    if filename.to_ascii_lowercase().ends_with(".png") || bytes.starts_with(PNG_SIG) {
        return parse_png_card(bytes);
    }
    parse_json_card(bytes, "json")
}

pub fn parse_json_card(bytes: &[u8], source_format: &str) -> AppResult<ParsedCard> {
    if bytes.len() > MAX_JSON_BYTES {
        return Err(AppError::bad_request(
            "CARD_TOO_LARGE",
            "character JSON exceeds size limit",
        ));
    }
    let raw: Value = serde_json::from_slice(bytes).map_err(|err| {
        AppError::bad_request("CARD_JSON_INVALID", format!("invalid card JSON: {err}"))
    })?;
    Ok(parsed_from_value(raw, source_format, Vec::new()))
}

pub fn parse_png_card(bytes: &[u8]) -> AppResult<ParsedCard> {
    if !bytes.starts_with(PNG_SIG) {
        return Err(AppError::bad_request("PNG_INVALID", "file is not a PNG"));
    }
    let mut cursor = Cursor::new(bytes);
    cursor.set_position(PNG_SIG.len() as u64);
    let mut chara = None;
    let mut ccv3 = None;
    loop {
        let mut len_buf = [0u8; 4];
        if cursor.read_exact(&mut len_buf).is_err() {
            break;
        }
        let length = u32::from_be_bytes(len_buf) as usize;
        let mut type_buf = [0u8; 4];
        cursor
            .read_exact(&mut type_buf)
            .map_err(|_| AppError::bad_request("PNG_INVALID", "truncated PNG chunk type"))?;
        if length > MAX_JSON_BYTES {
            return Err(AppError::bad_request(
                "PNG_CHUNK_TOO_LARGE",
                "PNG text chunk too large",
            ));
        }
        let mut data = vec![0u8; length];
        cursor
            .read_exact(&mut data)
            .map_err(|_| AppError::bad_request("PNG_INVALID", "truncated PNG chunk data"))?;
        let mut crc_buf = [0u8; 4];
        cursor
            .read_exact(&mut crc_buf)
            .map_err(|_| AppError::bad_request("PNG_INVALID", "truncated PNG CRC"))?;
        let mut crc_input = Vec::with_capacity(4 + data.len());
        crc_input.extend_from_slice(&type_buf);
        crc_input.extend_from_slice(&data);
        let expected = u32::from_be_bytes(crc_buf);
        let actual = crc32fast::hash(&crc_input);
        if expected != actual {
            return Err(AppError::bad_request(
                "PNG_CRC_MISMATCH",
                "PNG chunk CRC mismatch",
            ));
        }
        let chunk_type = std::str::from_utf8(&type_buf).unwrap_or("");
        if chunk_type == "IEND" {
            break;
        }
        if chunk_type == "tEXt" {
            if let Some((keyword, text)) = split_text_chunk(&data) {
                if keyword == "ccv3" {
                    ccv3 = Some(decode_card_text(&text)?);
                } else if keyword == "chara" {
                    chara = Some(decode_card_text(&text)?);
                }
            }
        }
    }
    if let Some(raw) = ccv3 {
        return Ok(parsed_from_value(raw, "png", Vec::new()));
    }
    if let Some(raw) = chara {
        return Ok(parsed_from_value(raw, "png", Vec::new()));
    }
    Err(AppError::bad_request(
        "PNG_CARD_MISSING",
        "PNG does not contain ccv3 or chara metadata",
    ))
}

pub fn encode_png_with_card(original_png: &[u8], raw: &Value) -> AppResult<Vec<u8>> {
    if !original_png.starts_with(PNG_SIG) {
        return Err(AppError::bad_request(
            "PNG_INVALID",
            "cannot export non-PNG asset as PNG",
        ));
    }
    let json = serde_json::to_vec(raw)
        .map_err(|err| AppError::internal(format!("serialize card: {err}")))?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(json);
    let mut output = Vec::from(PNG_SIG);
    let mut cursor = Cursor::new(original_png);
    cursor.set_position(PNG_SIG.len() as u64);
    loop {
        let mut header = [0u8; 8];
        if cursor.read_exact(&mut header).is_err() {
            break;
        }
        let length = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
        let chunk_type = &header[4..8];
        let mut data = vec![0u8; length];
        cursor
            .read_exact(&mut data)
            .map_err(|_| AppError::bad_request("PNG_INVALID", "truncated PNG while exporting"))?;
        let mut crc = [0u8; 4];
        cursor.read_exact(&mut crc).map_err(|_| {
            AppError::bad_request("PNG_INVALID", "truncated PNG CRC while exporting")
        })?;
        let kind = std::str::from_utf8(chunk_type).unwrap_or("");
        if kind == "tEXt" {
            if let Some((keyword, _)) = split_text_chunk(&data) {
                if keyword == "ccv3" || keyword == "chara" {
                    continue;
                }
            }
        }
        if kind == "IEND" {
            write_text_chunk(&mut output, "chara", &encoded);
            write_text_chunk(&mut output, "ccv3", &encoded);
        }
        output.extend_from_slice(&header);
        output.extend_from_slice(&data);
        output.extend_from_slice(&crc);
        if kind == "IEND" {
            break;
        }
    }
    Ok(output)
}

fn write_text_chunk(output: &mut Vec<u8>, keyword: &str, text: &str) {
    let mut data = Vec::new();
    data.extend_from_slice(keyword.as_bytes());
    data.push(0);
    data.extend_from_slice(text.as_bytes());
    let len = (data.len() as u32).to_be_bytes();
    let ty = b"tEXt";
    let mut crc_input = Vec::with_capacity(4 + data.len());
    crc_input.extend_from_slice(ty);
    crc_input.extend_from_slice(&data);
    let crc = crc32fast::hash(&crc_input).to_be_bytes();
    output.extend_from_slice(&len);
    output.extend_from_slice(ty);
    output.extend_from_slice(&data);
    output.extend_from_slice(&crc);
}

fn split_text_chunk(data: &[u8]) -> Option<(String, String)> {
    let zero = data.iter().position(|b| *b == 0)?;
    let keyword = std::str::from_utf8(&data[..zero]).ok()?.to_string();
    let text = std::str::from_utf8(&data[zero + 1..]).ok()?.to_string();
    Some((keyword, text))
}

fn decode_card_text(text: &str) -> AppResult<Value> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .map_err(|_| {
            AppError::bad_request("PNG_CARD_B64", "PNG card metadata is not valid Base64")
        })?;
    if decoded.len() > MAX_JSON_BYTES {
        return Err(AppError::bad_request(
            "CARD_TOO_LARGE",
            "decoded card JSON exceeds size limit",
        ));
    }
    serde_json::from_slice(&decoded).map_err(|err| {
        AppError::bad_request("PNG_CARD_JSON", format!("PNG card JSON invalid: {err}"))
    })
}

fn parsed_from_value(raw: Value, source_format: &str, mut warnings: Vec<String>) -> ParsedCard {
    let spec = raw
        .get("spec")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let spec_version = raw
        .get("spec_version")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    if spec.as_deref() == Some("chara_card_v3")
        || spec_version
            .as_deref()
            .map(|v| v.starts_with('3'))
            .unwrap_or(false)
    {
        // V3 extras are preserved in raw JSON even if unused by Prompt.
    } else if raw
        .get("data")
        .and_then(|data| data.get("character_book"))
        .is_some()
    {
        warnings.push("character_book is stored but not executed by legacy_bridge_v1".into());
    }
    let checksum = sha256_hex(serde_json::to_vec(&raw).unwrap_or_default().as_slice());
    ParsedCard {
        source_format: source_format.to_string(),
        spec,
        spec_version,
        raw,
        checksum,
        warnings,
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}
