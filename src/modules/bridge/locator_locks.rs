use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone)]
struct LockEntry {
    lock: Arc<Mutex<()>>,
    last_used: Instant,
}

pub struct LocatorLockManager {
    entries: Mutex<HashMap<String, LockEntry>>,
    capacity: usize,
    ttl: Duration,
}

impl LocatorLockManager {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
            ttl,
        }
    }

    pub async fn acquire(&self, key: &str) -> Result<OwnedMutexGuard<()>, &'static str> {
        if self.capacity == 0 {
            return Err("LOCATOR_LOCK_CAPACITY");
        }
        let lock = {
            let mut entries = self.entries.lock().await;
            let now = Instant::now();
            entries.retain(|_, e| {
                Arc::strong_count(&e.lock) > 1 || now.duration_since(e.last_used) < self.ttl
            });
            if !entries.contains_key(key) && entries.len() >= self.capacity {
                let victim = entries
                    .iter()
                    .filter(|(_, e)| Arc::strong_count(&e.lock) == 1)
                    .min_by_key(|(_, e)| e.last_used)
                    .map(|(k, _)| k.clone());
                if let Some(victim) = victim {
                    entries.remove(&victim);
                } else {
                    return Err("LOCATOR_LOCK_CAPACITY");
                }
            }
            let entry = entries.entry(key.to_string()).or_insert_with(|| LockEntry {
                lock: Arc::new(Mutex::new(())),
                last_used: now,
            });
            entry.last_used = now;
            Arc::clone(&entry.lock)
        };
        Ok(lock.lock_owned().await)
    }

    pub async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.entries.lock().await.is_empty()
    }

    pub async fn contains(&self, key: &str) -> bool {
        self.entries.lock().await.contains_key(key)
    }

    pub async fn active_count(&self) -> usize {
        let entries = self.entries.lock().await;
        entries
            .values()
            .filter(|e| Arc::strong_count(&e.lock) > 1)
            .count()
    }

    pub async fn prune(&self) -> usize {
        let mut entries = self.entries.lock().await;
        let now = Instant::now();
        let before = entries.len();
        entries.retain(|_, e| {
            Arc::strong_count(&e.lock) > 1 || now.duration_since(e.last_used) < self.ttl
        });
        before.saturating_sub(entries.len())
    }
}
