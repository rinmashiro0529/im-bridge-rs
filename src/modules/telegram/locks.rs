use tokio::sync::OwnedMutexGuard;

crate::lock_table::define_lock_manager!(DeliveryLockManager);

impl DeliveryLockManager {
    pub async fn acquire(&self, key: String) -> Result<OwnedMutexGuard<()>, &'static str> {
        self.table
            .acquire(key)
            .await
            .map_err(|_| "DELIVERY_LOCK_CAPACITY")
    }
}
