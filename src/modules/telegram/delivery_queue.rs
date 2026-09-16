use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

const DEFAULT_MAX_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeliveryKey {
    bot_id: String,
    chat_id: Option<i64>,
}

#[derive(Debug)]
struct CooldownState {
    cooldown_until: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    pub delivery_id: String,
    pub bot_id: String,
    pub chat_id: i64,
}

/// Shared Telegram rate-limit state. A bot-wide deadline and a chat-specific
/// deadline are both tracked; a request waits for whichever deadline is later.
#[derive(Clone)]
pub struct DeliveryCoordinator {
    states: Arc<Mutex<HashMap<DeliveryKey, CooldownState>>>,
    pending: Arc<Mutex<HashMap<String, PendingDelivery>>>,
    notify: Arc<Notify>,
    max_retry_after: Duration,
}

impl Default for DeliveryCoordinator {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_RETRY_AFTER)
    }
}

impl DeliveryCoordinator {
    pub fn new(max_retry_after: Duration) -> Self {
        Self {
            states: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            max_retry_after: max_retry_after.max(Duration::from_secs(1)),
        }
    }

    pub fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<DeliveryCoordinator>> = OnceLock::new();
        GLOBAL.get_or_init(|| Arc::new(Self::default())).clone()
    }

    pub async fn wait(&self, bot_id: &str, chat_id: i64) {
        loop {
            let wait = {
                let states = self.states.lock().await;
                let now = Instant::now();
                let bot_until = states
                    .get(&DeliveryKey {
                        bot_id: bot_id.to_string(),
                        chat_id: None,
                    })
                    .map(|state| state.cooldown_until)
                    .unwrap_or(now);
                let chat_until = states
                    .get(&DeliveryKey {
                        bot_id: bot_id.to_string(),
                        chat_id: Some(chat_id),
                    })
                    .map(|state| state.cooldown_until)
                    .unwrap_or(now);
                bot_until.max(chat_until).saturating_duration_since(now)
            };
            if wait.is_zero() {
                return;
            }
            tokio::time::sleep(wait).await;
        }
    }

    pub async fn set_cooldown_secs(&self, bot_id: &str, chat_id: i64, retry_after_secs: u64) {
        if retry_after_secs > self.max_retry_after.as_secs() {
            tracing::warn!(
                bot_id,
                chat_id,
                retry_after_secs,
                max_retry_after_secs = self.max_retry_after.as_secs(),
                "telegram retry_after exceeded local advisory cap; preserving server deadline"
            );
        }
        let retry_after = Duration::from_secs(retry_after_secs);
        let until = Instant::now()
            .checked_add(retry_after)
            .unwrap_or_else(|| Instant::now() + self.max_retry_after);
        let mut states = self.states.lock().await;
        for key in [
            DeliveryKey {
                bot_id: bot_id.to_string(),
                chat_id: None,
            },
            DeliveryKey {
                bot_id: bot_id.to_string(),
                chat_id: Some(chat_id),
            },
        ] {
            let entry = states.entry(key).or_insert(CooldownState {
                cooldown_until: Instant::now(),
            });
            if until > entry.cooldown_until {
                entry.cooldown_until = until;
            }
        }
        states.retain(|_, state| state.cooldown_until > Instant::now());
    }

    pub async fn enqueue_pending(
        &self,
        delivery_id: impl Into<String>,
        bot_id: &str,
        chat_id: i64,
    ) {
        let pending = PendingDelivery {
            delivery_id: delivery_id.into(),
            bot_id: bot_id.to_string(),
            chat_id,
        };
        self.pending
            .lock()
            .await
            .insert(pending.delivery_id.clone(), pending);
        self.notify.notify_one();
    }

    pub async fn take_due(&self) -> Option<PendingDelivery> {
        let pending = {
            let pending = self.pending.lock().await;
            pending.values().find_map(|item| {
                self.cooldown_remaining(&item.bot_id, item.chat_id)
                    .is_zero()
                    .then(|| item.clone())
            })
        }?;
        self.pending.lock().await.remove(&pending.delivery_id)
    }

    pub async fn take_due_for(&self, bot_id: &str, chat_id: i64) -> Option<PendingDelivery> {
        let pending = {
            let pending = self.pending.lock().await;
            pending.values().find_map(|item| {
                (item.bot_id == bot_id
                    && item.chat_id == chat_id
                    && self
                        .cooldown_remaining(&item.bot_id, item.chat_id)
                        .is_zero())
                .then(|| item.clone())
            })
        }?;
        self.pending.lock().await.remove(&pending.delivery_id)
    }

    pub async fn wait_for_pending(&self) -> PendingDelivery {
        loop {
            if let Some(pending) = self.take_due().await {
                return pending;
            }
            let notified = self.notify.notified();
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    pub async fn forget_pending(&self, delivery_id: &str) {
        self.pending.lock().await.remove(delivery_id);
    }

    pub fn is_cooling_down(&self, bot_id: &str, chat_id: i64) -> bool {
        self.cooldown_remaining(bot_id, chat_id) > Duration::ZERO
    }

    pub fn cooldown_remaining(&self, bot_id: &str, chat_id: i64) -> Duration {
        let now = Instant::now();
        let states = self.states.try_lock();
        let Ok(states) = states else {
            return self.max_retry_after;
        };
        let bot_until = states
            .get(&DeliveryKey {
                bot_id: bot_id.to_string(),
                chat_id: None,
            })
            .map(|state| state.cooldown_until)
            .unwrap_or(now);
        let chat_until = states
            .get(&DeliveryKey {
                bot_id: bot_id.to_string(),
                chat_id: Some(chat_id),
            })
            .map(|state| state.cooldown_until)
            .unwrap_or(now);
        bot_until.max(chat_until).saturating_duration_since(now)
    }
}
