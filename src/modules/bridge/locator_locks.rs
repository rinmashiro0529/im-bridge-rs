use tokio::sync::OwnedMutexGuard;

crate::lock_table::define_lock_manager!(LocatorLockManager);

impl LocatorLockManager {
    pub async fn acquire(&self, key: &str) -> Result<OwnedMutexGuard<()>, &'static str> {
        self.table
            .acquire(key.to_string())
            .await
            .map_err(|_| "LOCATOR_LOCK_CAPACITY")
    }
}
