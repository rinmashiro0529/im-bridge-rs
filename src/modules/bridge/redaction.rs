const SENSITIVE_KEYS: &[&str] = &[
    "authorization",
    "cookie",
    "x-csrf-token",
    "csrf-token",
    "csrf_token",
    "csrf",
    "bot-token",
    "bot_token",
    "bot token",
    "api-key",
    "api key",
    "api_key",
    "apikey",
    "access-token",
    "access_token",
    "provider-key",
    "provider_key",
    "password",
    "token",
    "secret",
];

const REDACTED: &str = "[REDACTED]";
const PATH_REDACTED: &str = "[PATH_REDACTED]";

pub fn redact_detail(input: &str) -> Option<String> {
    if input.trim().is_empty() {
        return None;
    }
    let mut value = input.to_string();
    for key in SENSITIVE_KEYS {
        value = redact_key_value(value, key);
    }
    value = redact_url_userinfo(value);
    value = redact_token_like(value);
    value = redact_sensitive_paths(value);
    let value: String = value.chars().take(300).collect();
    let lowered = value.to_ascii_lowercase();
    if lowered.contains("prompt")
        || lowered.contains("chat_metadata")
        || lowered.contains("content_original")
        || lowered.contains("\"mes\"")
    {
        return None;
    }
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn redact_key_value(mut text: String, key: &str) -> String {
    let mut search_offset = 0;
    while search_offset < text.len() {
        let Some(relative) = find_case_insensitive(&text[search_offset..], key) else {
            break;
        };
        let key_start = search_offset + relative;
        let key_end = key_start + key.len();
        if !is_key_boundary(&text, key_start, key_end) {
            search_offset = key_end;
            continue;
        }
        let Some((value_start, value_quote)) = locate_value_start(&text, key_end) else {
            search_offset = key_end;
            continue;
        };
        let value_end = if let Some(quote) = value_quote {
            scan_quoted_end(&text, value_start, quote)
        } else if key.eq_ignore_ascii_case("cookie") {
            scan_line_end(&text, value_start)
        } else {
            let first_end = scan_unquoted_end(&text, value_start);
            if key.eq_ignore_ascii_case("authorization") {
                authorization_value_end(&text, value_start, first_end)
            } else {
                first_end
            }
        };
        if value_end <= value_start {
            search_offset = value_start.saturating_add(1);
            continue;
        }
        text.replace_range(value_start..value_end, REDACTED);
        search_offset = value_start + REDACTED.len();
    }
    text
}

fn is_key_boundary(text: &str, key_start: usize, key_end: usize) -> bool {
    let starts_inside_word = text[..key_start]
        .chars()
        .next_back()
        .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_');
    let ends_inside_word = text[key_end..]
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_');
    !starts_inside_word && !ends_inside_word
}

fn locate_value_start(text: &str, key_end: usize) -> Option<(usize, Option<char>)> {
    let mut cursor = skip_whitespace(text, key_end);
    if let Some(quote) = text[cursor..]
        .chars()
        .next()
        .filter(|character| matches!(character, '"' | '\''))
    {
        cursor += quote.len_utf8();
        cursor = skip_whitespace(text, cursor);
    }
    let delimiter = text[cursor..].chars().next()?;
    if !matches!(delimiter, ':' | '=') {
        return None;
    }
    cursor += delimiter.len_utf8();
    cursor = skip_whitespace(text, cursor);
    let value_quote = text[cursor..]
        .chars()
        .next()
        .filter(|character| matches!(character, '"' | '\''));
    if let Some(quote) = value_quote {
        cursor += quote.len_utf8();
    }
    Some((cursor, value_quote))
}

fn authorization_value_end(text: &str, value_start: usize, first_end: usize) -> usize {
    let scheme = &text[value_start..first_end];
    if !scheme.eq_ignore_ascii_case("bearer") && !scheme.eq_ignore_ascii_case("basic") {
        return first_end;
    }
    let credential_start = skip_whitespace(text, first_end);
    let credential_end = scan_unquoted_end(text, credential_start);
    if credential_end > credential_start {
        credential_end
    } else {
        first_end
    }
}

fn skip_whitespace(text: &str, mut offset: usize) -> usize {
    while text[offset..]
        .chars()
        .next()
        .is_some_and(|character| character.is_whitespace())
    {
        offset += text[offset..].chars().next().unwrap().len_utf8();
    }
    offset
}

fn scan_unquoted_end(text: &str, value_start: usize) -> usize {
    let mut value_end = value_start;
    for (index, character) in text[value_start..].char_indices() {
        if character.is_whitespace()
            || matches!(character, ',' | ';' | '&' | '#' | '}' | ']' | '"' | '\'')
        {
            return value_start + index;
        }
        value_end = value_start + index + character.len_utf8();
    }
    value_end
}

fn scan_line_end(text: &str, value_start: usize) -> usize {
    text[value_start..]
        .find(['\r', '\n'])
        .map(|relative| value_start + relative)
        .unwrap_or(text.len())
}

fn scan_quoted_end(text: &str, value_start: usize, quote: char) -> usize {
    let mut escaped = false;
    for (index, character) in text[value_start..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if character == quote {
            return value_start + index;
        }
    }
    text.len()
}

fn redact_url_userinfo(mut text: String) -> String {
    let mut offset = 0;
    while offset < text.len() {
        let Some(relative) = text[offset..].find("://") else {
            break;
        };
        let scheme_end = offset + relative;
        let authority_start = scheme_end + 3;
        let authority_end = text[authority_start..]
            .find(|character: char| {
                character.is_whitespace() || matches!(character, '/' | '"' | '\'')
            })
            .map(|index| authority_start + index)
            .unwrap_or(text.len());
        let Some(at_offset) = text[authority_start..authority_end].find('@') else {
            offset = authority_end;
            continue;
        };
        let at = authority_start + at_offset;
        if at > authority_start {
            text.replace_range(authority_start..at, REDACTED);
            offset = authority_start + REDACTED.len() + 1;
        } else {
            offset = authority_end;
        }
    }
    text
}

fn redact_token_like(mut text: String) -> String {
    let mut ranges = Vec::new();
    let mut token_start = None;
    let mut has_alpha = false;
    let mut has_digit = false;
    for (index, character) in text
        .char_indices()
        .chain(std::iter::once((text.len(), '\0')))
    {
        let token_character = character.is_ascii_alphanumeric() || matches!(character, '_' | '-');
        if token_character && token_start.is_none() {
            token_start = Some(index);
            has_alpha = false;
            has_digit = false;
        }
        if token_character {
            has_alpha |= character.is_ascii_alphabetic();
            has_digit |= character.is_ascii_digit();
        } else if let Some(start) = token_start.take() {
            if index.saturating_sub(start) >= 32 && has_alpha && has_digit {
                ranges.push((start, index));
            }
        }
    }
    for (start, end) in ranges.into_iter().rev() {
        text.replace_range(start..end, REDACTED);
    }
    text
}

fn redact_sensitive_paths(mut text: String) -> String {
    let mut ranges = Vec::new();
    let bytes = text.as_bytes();
    for index in 0..bytes.len() {
        let drive_path = index + 2 < bytes.len()
            && bytes[index].is_ascii_alphabetic()
            && bytes[index + 1] == b':'
            && (bytes[index + 2] == b'\\' || bytes[index + 2] == b'/');
        let unc_path = is_unc_path(bytes, index);
        let unix_path = [
            b"/srv/".as_slice(),
            b"/home/".as_slice(),
            b"/var/".as_slice(),
            b"/root/".as_slice(),
            b"/opt/".as_slice(),
            b"/etc/".as_slice(),
            b"/users/".as_slice(),
            b"/private/".as_slice(),
        ]
        .iter()
        .any(|prefix| bytes[index..].starts_with(prefix));
        if (!drive_path && !unc_path && !unix_path) || !is_path_boundary(bytes, index) {
            continue;
        }
        let end = text[index..]
            .find(|character: char| {
                character.is_whitespace()
                    || matches!(
                        character,
                        ',' | ';' | '&' | '#' | ')' | ']' | '}' | '"' | '\''
                    )
            })
            .map(|relative| index + relative)
            .unwrap_or(text.len());
        ranges.push((index, end));
    }
    for (start, end) in ranges.into_iter().rev() {
        text.replace_range(start..end, PATH_REDACTED);
    }
    text
}

fn is_path_boundary(bytes: &[u8], index: usize) -> bool {
    index == 0
        || (!bytes[index - 1].is_ascii_alphanumeric()
            && bytes[index - 1] != b'_'
            && bytes[index - 1] != b'/'
            && bytes[index - 1] != b'\\')
}

fn is_unc_path(bytes: &[u8], index: usize) -> bool {
    if !bytes[index..].starts_with(b"\\\\") || index + 2 >= bytes.len() {
        return false;
    }
    let server_start = index + 2;
    let Some(server_separator) = bytes[server_start..]
        .iter()
        .position(|byte| matches!(*byte, b'\\' | b'/'))
    else {
        return false;
    };
    if server_separator == 0 {
        return false;
    }
    let share_start = server_start + server_separator + 1;
    share_start < bytes.len() && !matches!(bytes[share_start], b'\\' | b'/' | b' ' | b'\t')
}

fn find_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    haystack
        .to_ascii_lowercase()
        .find(&needle.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::redact_detail;

    #[test]
    fn redacts_headers_urls_tokens_and_paths() {
        let input = "Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789; Cookie=sid-value; https://user:pass@example.test/api?token=query-secret C:\\Users\\Synthetic\\secret.db";
        let detail = redact_detail(input).expect("safe detail");
        assert!(!detail.contains("Bearer"));
        assert!(!detail.contains("sid-value"));
        assert!(!detail.contains("user:pass"));
        assert!(!detail.contains("query-secret"));
        assert!(!detail.contains("C:\\Users"));
    }

    #[test]
    fn redacts_json_header_values() {
        let detail = redact_detail(
            r#"{"authorization":"Bearer json-secret","cookie":"json-cookie","csrf":"json-csrf","bot_token":"json-bot","api_key":"json-key","access_token":"json-access"}"#,
        )
        .expect("safe detail");
        for secret in [
            "json-secret",
            "json-cookie",
            "json-csrf",
            "json-bot",
            "json-key",
            "json-access",
        ] {
            assert!(!detail.contains(secret), "secret remained: {secret}");
        }
    }

    #[test]
    fn redacts_sensitive_unix_and_unc_paths() {
        let detail = redact_detail(
            r#"/root/private.db /opt/app/config /etc/secret.conf \\server\share\private.db"#,
        )
        .expect("safe detail");
        assert!(!detail.contains("/root/"));
        assert!(!detail.contains("/opt/"));
        assert!(!detail.contains("/etc/"));
        assert!(!detail.contains("\\\\server"));
    }

    #[test]
    fn bounds_detail_length_and_rejects_prompt_or_chat_shapes() {
        let detail = redact_detail(&"ordinary ".repeat(100)).expect("safe detail");
        assert!(detail.chars().count() <= 300);
        assert!(redact_detail(r#"{"prompt":"private"}"#).is_none());
        assert!(redact_detail(r#"{"mes":"private chat"}"#).is_none());
    }
}
