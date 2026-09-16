use crate::domain::conversation::{HistoryItem, Message, MessageRole};

pub fn history_items(messages: &[Message]) -> Vec<HistoryItem> {
    messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
        .map(|message| HistoryItem {
            message_id: message.id.clone(),
            turn_id: message.turn_id.clone(),
            speaker: message
                .metadata
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or(if message.role == MessageRole::User {
                    "User"
                } else {
                    "Character"
                })
                .to_string(),
            text: message.prompt_text().to_string(),
            send_date: message
                .metadata
                .get("send_date")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            is_user: message.role == MessageRole::User,
            sort_index: message.sequence,
        })
        .collect()
}
