use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedMutexGuard};

struct LockEntry {
    lock: Arc<Mutex<()>>,
    last_used: Instant,
}

#[derive(Debug)]
pub(crate) struct AtCapacity;

pub(crate) struct LockTable {
    entries: Mutex<HashMap<String, LockEntry>>,
    capacity: usize,
    ttl: Duration,
}

impl LockTable {
    pub(crate) fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
            ttl,
        }
    }

    pub(crate) async fn acquire(&self, key: String) -> Result<OwnedMutexGuard<()>, AtCapacity> {
        if self.capacity == 0 {
            return Err(AtCapacity);
        }
        let lock = {
            let mut entries = self.entries.lock().await;
            let now = Instant::now();
            entries.retain(|_, e| {
                Arc::strong_count(&e.lock) > 1 || now.duration_since(e.last_used) < self.ttl
            });
            if !entries.contains_key(&key) && entries.len() >= self.capacity {
                let victim = entries
                    .iter()
                    .filter(|(_, e)| Arc::strong_count(&e.lock) == 1)
                    .min_by_key(|(_, e)| e.last_used)
                    .map(|(k, _)| k.clone())
                    .ok_or(AtCapacity)?;
                entries.remove(&victim);
            }
            let entry = entries.entry(key).or_insert_with(|| LockEntry {
                lock: Arc::new(Mutex::new(())),
                last_used: now,
            });
            entry.last_used = now;
            // Clone while the table is locked: waiters must also prevent eviction.
            Arc::clone(&entry.lock)
        };
        // Never hold the table mutex while waiting for a key's mutex.
        Ok(lock.lock_owned().await)
    }

    pub(crate) async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    pub(crate) async fn contains(&self, key: &str) -> bool {
        self.entries.lock().await.contains_key(key)
    }

    pub(crate) async fn active_count(&self) -> usize {
        self.entries
            .lock()
            .await
            .values()
            .filter(|e| Arc::strong_count(&e.lock) > 1)
            .count()
    }

    pub(crate) async fn prune(&self) -> usize {
        let mut entries = self.entries.lock().await;
        let now = Instant::now();
        let before = entries.len();
        entries.retain(|_, e| {
            Arc::strong_count(&e.lock) > 1 || now.duration_since(e.last_used) < self.ttl
        });
        before.saturating_sub(entries.len())
    }
}

// Only generate the existing forwarding API. The algorithm above is concrete,
// uses String keys, and contains no policy or backend generics. Each wrapper
// owns a distinct table; this macro never introduces shared/global lock state.
macro_rules! define_lock_manager {
    ($name:ident) => {
        pub struct $name {
            table: $crate::lock_table::LockTable,
        }

        impl $name {
            pub fn new(capacity: usize, ttl: std::time::Duration) -> Self {
                Self {
                    table: $crate::lock_table::LockTable::new(capacity, ttl),
                }
            }

            pub async fn len(&self) -> usize {
                self.table.len().await
            }

            pub async fn is_empty(&self) -> bool {
                self.table.len().await == 0
            }

            pub async fn contains(&self, key: &str) -> bool {
                self.table.contains(key).await
            }

            pub async fn active_count(&self) -> usize {
                self.table.active_count().await
            }

            pub async fn prune(&self) -> usize {
                self.table.prune().await
            }
        }
    };
}

pub(crate) use define_lock_manager;
