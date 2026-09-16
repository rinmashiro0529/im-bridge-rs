#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StSseEvent {
    Heartbeat,
    Message { event: Option<String>, data: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseDecodeError {
    LineTooLarge,
    EventTooLarge,
    InvalidUtf8,
    InvalidJson,
    InvalidPayload,
    UnsupportedOutput,
    Truncated,
}

impl std::fmt::Display for SseDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LineTooLarge => write!(f, "SSE line exceeded maximum size"),
            Self::EventTooLarge => write!(f, "SSE event exceeded maximum size"),
            Self::InvalidUtf8 => write!(f, "SSE stream contains invalid UTF-8"),
            Self::InvalidJson => write!(f, "SSE payload contains invalid JSON"),
            Self::InvalidPayload => write!(f, "SSE payload has an invalid shape"),
            Self::UnsupportedOutput => write!(f, "SSE payload contains unsupported output"),
            Self::Truncated => write!(f, "SSE stream was truncated unexpectedly"),
        }
    }
}

impl std::error::Error for SseDecodeError {}

pub struct StSseStreamDecoder {
    line: Vec<u8>,
    data: Vec<String>,
    event: Option<String>,
    event_bytes: usize,
    skip_lf: bool,
    max_line: usize,
    max_event: usize,
    bom_prefix: Vec<u8>,
    bom_checked: bool,
}

impl StSseStreamDecoder {
    pub fn new(max_line: usize, max_event: usize) -> Self {
        Self {
            line: Vec::new(),
            data: Vec::new(),
            event: None,
            event_bytes: 0,
            skip_lf: false,
            max_line,
            max_event,
            bom_prefix: Vec::new(),
            bom_checked: false,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<StSseEvent>, SseDecodeError> {
        let mut out = Vec::new();
        let mut slice = bytes;
        if !self.bom_checked {
            let needed = 3usize.saturating_sub(self.bom_prefix.len());
            let take = needed.min(slice.len());
            self.bom_prefix.extend_from_slice(&slice[..take]);
            slice = &slice[take..];
            if self.bom_prefix.len() < 3 {
                return Ok(out);
            }
            self.bom_checked = true;
            if self.bom_prefix != [0xEF, 0xBB, 0xBF] {
                let prefix = std::mem::take(&mut self.bom_prefix);
                self.process_bytes(&prefix, &mut out)?;
            } else {
                self.bom_prefix.clear();
            }
        }
        self.process_bytes(slice, &mut out)?;
        Ok(out)
    }

    fn process_bytes(
        &mut self,
        bytes: &[u8],
        out: &mut Vec<StSseEvent>,
    ) -> Result<(), SseDecodeError> {
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                self.finish_line(out)?;
                self.skip_lf = byte == b'\r';
            } else {
                if self.line.len() >= self.max_line {
                    return Err(SseDecodeError::LineTooLarge);
                }
                self.line.push(byte);
            }
        }
        Ok(())
    }

    fn finish_line(&mut self, out: &mut Vec<StSseEvent>) -> Result<(), SseDecodeError> {
        let bytes = std::mem::take(&mut self.line);
        let line = std::str::from_utf8(&bytes).map_err(|_| SseDecodeError::InvalidUtf8)?;
        if line.is_empty() {
            if !self.data.is_empty() || self.event.is_some() {
                out.push(StSseEvent::Message {
                    event: self.event.take(),
                    data: self.data.join("\n"),
                });
            }
            self.data.clear();
            self.event = None;
            self.event_bytes = 0;
            return Ok(());
        }
        if line.starts_with(':') {
            out.push(StSseEvent::Heartbeat);
            return Ok(());
        }
        self.event_bytes = self
            .event_bytes
            .checked_add(line.len() + 1)
            .ok_or(SseDecodeError::EventTooLarge)?;
        if self.event_bytes > self.max_event {
            return Err(SseDecodeError::EventTooLarge);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => self.data.push(value.to_owned()),
            "event" => self.event = Some(value.to_owned()),
            _ => {}
        }
        Ok(())
    }

    pub fn finish(self) -> Result<(), SseDecodeError> {
        if !self.bom_checked && !self.bom_prefix.is_empty() {
            return Err(SseDecodeError::Truncated);
        }
        if self.line.is_empty() && self.data.is_empty() && self.event.is_none() {
            Ok(())
        } else {
            Err(SseDecodeError::Truncated)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StGenerationEvent {
    Ignored,
    TextDelta { sequence: u64, text: String },
    Finished { finish_reason: Option<String> },
    Rejected { safe_code: String },
}

impl StGenerationEvent {
    pub fn is_accepted_finish(&self) -> bool {
        match self {
            Self::Finished { finish_reason } => matches!(
                finish_reason.as_deref(),
                Some("stop") | Some("eos") | Some("end_turn")
            ),
            _ => false,
        }
    }
}

pub fn decode_st_generation_event(
    data: &str,
    sequence: u64,
) -> Result<Vec<StGenerationEvent>, SseDecodeError> {
    let trimmed = data.trim();
    if trimmed.is_empty() {
        return Ok(vec![StGenerationEvent::Ignored]);
    }
    if trimmed == "[DONE]" {
        return Ok(vec![StGenerationEvent::Finished {
            finish_reason: Some("stop".into()),
        }]);
    }
    let parsed: serde_json::Value =
        serde_json::from_str(trimmed).map_err(|_| SseDecodeError::InvalidJson)?;

    if parsed.get("error").is_some() {
        return Ok(vec![StGenerationEvent::Rejected {
            safe_code: "ST_GENERATION_FAILED".into(),
        }]);
    }
    let parsed = parsed.as_object().ok_or(SseDecodeError::InvalidPayload)?;
    let choices = parsed
        .get("choices")
        .and_then(serde_json::Value::as_array)
        .ok_or(SseDecodeError::UnsupportedOutput)?;
    if choices.is_empty() {
        let allowed_metadata = [
            "choices",
            "usage",
            "id",
            "object",
            "created",
            "model",
            "system_fingerprint",
        ];
        let only_metadata = parsed
            .keys()
            .all(|key| allowed_metadata.contains(&key.as_str()));
        let has_metadata = parsed.keys().any(|key| key != "choices");
        if only_metadata && has_metadata {
            return Ok(vec![StGenerationEvent::Ignored]);
        }
        return Err(SseDecodeError::InvalidPayload);
    }
    let choice = choices.first().ok_or(SseDecodeError::InvalidPayload)?;
    let choice = choice.as_object().ok_or(SseDecodeError::InvalidPayload)?;
    if choice.get("message").is_some() {
        return Err(SseDecodeError::UnsupportedOutput);
    }
    let delta = choice
        .get("delta")
        .and_then(serde_json::Value::as_object)
        .ok_or(SseDecodeError::InvalidPayload)?;
    for (key, value) in delta {
        match key.as_str() {
            "role" => {}
            "content" => {
                if !value.is_null() && value.as_str().is_none() {
                    return Err(SseDecodeError::InvalidPayload);
                }
            }
            "reasoning_content" => {
                if !value.is_null() && value.as_str().is_none() {
                    return Err(SseDecodeError::InvalidPayload);
                }
            }
            "tool_calls" | "function_call" => {
                return Ok(vec![StGenerationEvent::Rejected {
                    safe_code: "ST_GENERATION_UNSUPPORTED_OUTPUT".into(),
                }]);
            }
            _ => return Err(SseDecodeError::UnsupportedOutput),
        }
    }

    let mut events = Vec::new();
    if let Some(content) = delta.get("content") {
        if !content.is_null() {
            let content = content.as_str().ok_or(SseDecodeError::InvalidPayload)?;
            if !content.is_empty() {
                events.push(StGenerationEvent::TextDelta {
                    sequence,
                    text: content.to_owned(),
                });
            }
        }
    }

    if let Some(finish_reason) = choice.get("finish_reason") {
        if !finish_reason.is_null() {
            let reason = finish_reason
                .as_str()
                .ok_or(SseDecodeError::InvalidPayload)?;
            if reason.trim().is_empty() {
                return Err(SseDecodeError::InvalidPayload);
            }
            events.push(StGenerationEvent::Finished {
                finish_reason: Some(reason.to_owned()),
            });
        }
    }

    if events.is_empty() {
        return Ok(vec![StGenerationEvent::Ignored]);
    }
    Ok(events)
}
