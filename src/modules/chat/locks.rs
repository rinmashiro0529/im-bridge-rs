use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Default)]
pub struct ConversationLocks {
    inner: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl ConversationLocks {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn lock(&self, conversation_id: &str) -> OwnedMutexGuard<()> {
        let handle = {
            let mut map = self.inner.lock().await;
            map.entry(conversation_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        handle.lock_owned().await
    }
}
