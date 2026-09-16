use std::time::Instant;

use crate::modules::telegram::TelegramModule;

#[derive(Debug, Clone)]
pub struct StreamPolicy {
    pub min_interval_ms: u64,
    pub min_delta_chars: usize,
    pub first_render_chars: usize,
    pub chunk_size: usize,
}

impl Default for StreamPolicy {
    fn default() -> Self {
        Self {
            min_interval_ms: 5000,
            min_delta_chars: 700,
            first_render_chars: 300,
            chunk_size: 3200,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamState {
    last_rendered: String,
    last_render_at: Option<Instant>,
    last_committed_len: usize,
}

impl StreamState {
    pub fn new() -> Self {
        Self {
            last_rendered: String::new(),
            last_render_at: None,
            last_committed_len: 0,
        }
    }

    pub fn should_render(&mut self, full_text: &str, policy: &StreamPolicy, now: Instant) -> bool {
        if full_text.is_empty() || full_text == self.last_rendered {
            return false;
        }
        if self.last_committed_len == 0 && full_text.chars().count() < policy.first_render_chars {
            return false;
        }
        let delta = full_text
            .chars()
            .count()
            .saturating_sub(self.last_committed_len);
        if let Some(last) = self.last_render_at {
            if now.duration_since(last).as_millis() < u128::from(policy.min_interval_ms)
                && delta < policy.min_delta_chars
            {
                return false;
            }
        }
        self.commit(full_text, now);
        true
    }

    pub fn force_final(&mut self, full_text: &str, now: Instant) -> bool {
        if full_text.is_empty() || full_text == self.last_rendered {
            return false;
        }
        self.commit(full_text, now);
        true
    }

    fn commit(&mut self, full_text: &str, now: Instant) {
        self.last_rendered = full_text.to_string();
        self.last_committed_len = full_text.chars().count();
        self.last_render_at = Some(now);
    }
}

pub fn chunk_text(text: &str, chunk_size: usize) -> Vec<String> {
    TelegramModule::split_text(text, chunk_size.max(1))
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn waits_for_first_render_threshold() {
        let mut state = StreamState::new();
        let policy = StreamPolicy::default();
        let now = Instant::now();
        assert!(!state.should_render("abc", &policy, now));
        let long = "x".repeat(policy.first_render_chars);
        assert!(state.should_render(&long, &policy, now));
        assert!(!state.should_render(&long, &policy, now));
    }

    #[test]
    fn throttles_small_deltas_inside_interval() {
        let mut state = StreamState::new();
        let policy = StreamPolicy {
            min_interval_ms: 5_000,
            min_delta_chars: 700,
            first_render_chars: 3,
            chunk_size: 32,
        };
        let now = Instant::now();
        assert!(state.should_render("abcd", &policy, now));
        assert!(!state.should_render("abcde", &policy, now + Duration::from_millis(10)));
        let later = now + Duration::from_millis(5_001);
        assert!(state.should_render("abcde", &policy, later));
    }
}
