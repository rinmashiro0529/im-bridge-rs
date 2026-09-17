use std::task::Poll;
use std::time::Duration;

use im_bridge::modules::bridge::locator_locks::LocatorLockManager;
use im_bridge::modules::telegram::locks::DeliveryLockManager;

fn borrowed_key(value: &str) -> &str {
    value
}

// Run the same contract against both public APIs, not only the private core.
// Each generated module supplies the appropriate owned/borrowed key adapter.
macro_rules! lock_contract {
    ($suite:ident, $manager:ident, $key:ty, $convert:expr, $error:literal) => {
        mod $suite {
            use super::*;

            fn key(value: &str) -> $key {
                $convert(value)
            }

            #[tokio::test]
            async fn zero_capacity_preserves_error_code_and_empty_state() {
                let manager = $manager::new(0, Duration::ZERO);
                assert_eq!(manager.acquire(key("one")).await.unwrap_err(), $error);
                assert_eq!(manager.len().await, 0);
                assert!(manager.is_empty().await);
                assert_eq!(manager.active_count().await, 0);
                assert!(!manager.contains("one").await);
                assert_eq!(manager.prune().await, 0);
            }

            #[tokio::test]
            async fn same_key_serializes_and_other_keys_do_not_wait() {
                let manager = $manager::new(2, Duration::from_secs(60));
                let first = manager.acquire(key("one")).await.unwrap();
                let mut waiter = Box::pin(manager.acquire(key("one")));
                assert!(matches!(futures_util::poll!(&mut waiter), Poll::Pending));
                let other = tokio::time::timeout(
                    Duration::from_secs(2),
                    manager.acquire(key("two")),
                )
                .await
                .expect("unrelated key blocked")
                .unwrap();
                assert_eq!(manager.len().await, 2);
                assert_eq!(manager.active_count().await, 2);
                drop(first);
                let replacement = tokio::time::timeout(Duration::from_secs(2), waiter)
                    .await
                    .expect("same-key waiter never acquired after release")
                    .unwrap();
                assert_eq!(manager.active_count().await, 2);
                drop((replacement, other));
                assert_eq!(manager.active_count().await, 0);
            }

            #[tokio::test]
            async fn a_waiter_alone_prevents_eviction_until_cancelled() {
                let manager = $manager::new(1, Duration::ZERO);
                let holder = manager.acquire(key("one")).await.unwrap();
                let mut waiter = Box::pin(manager.acquire(key("one")));
                assert!(matches!(futures_util::poll!(&mut waiter), Poll::Pending));
                drop(holder);
                assert_eq!(manager.prune().await, 0);
                assert_eq!(manager.acquire(key("two")).await.unwrap_err(), $error);
                assert!(manager.contains("one").await);
                drop(waiter);
                assert_eq!(manager.active_count().await, 0);
                assert_eq!(manager.prune().await, 1);
                assert!(manager.is_empty().await);
                assert!(manager.acquire(key("two")).await.is_ok());
            }

            #[tokio::test]
            async fn expired_idle_entries_are_removed_but_live_entries_survive() {
                let manager = $manager::new(2, Duration::ZERO);
                let live = manager.acquire(key("live")).await.unwrap();
                drop(manager.acquire(key("idle")).await.unwrap());
                assert_eq!(manager.prune().await, 1);
                assert_eq!(manager.len().await, 1);
                assert!(manager.contains("live").await);
                assert!(!manager.contains("idle").await);
                let next = manager.acquire(key("next")).await.unwrap();
                assert_eq!(manager.acquire(key("full")).await.unwrap_err(), $error);
                drop((live, next));
                assert_eq!(manager.prune().await, 2);
            }

            #[tokio::test]
            async fn capacity_evicts_the_oldest_idle_entry_before_its_ttl() {
                let manager = $manager::new(2, Duration::from_secs(60));
                drop(manager.acquire(key("old")).await.unwrap());
                // The implementation uses std::Instant, not Tokio's virtual clock.
                tokio::time::sleep(Duration::from_millis(2)).await;
                drop(manager.acquire(key("newer")).await.unwrap());
                let guard = manager.acquire(key("third")).await.unwrap();
                assert!(!manager.contains("old").await);
                assert!(manager.contains("newer").await);
                assert!(manager.contains("third").await);
                assert_eq!(manager.len().await, 2);
                drop(guard);
            }

            #[tokio::test]
            async fn separate_instances_do_not_share_entries_or_capacity() {
                let left = $manager::new(1, Duration::ZERO);
                let right = $manager::new(1, Duration::ZERO);
                let first = left.acquire(key("same")).await.unwrap();
                let second = tokio::time::timeout(
                    Duration::from_secs(2),
                    right.acquire(key("same")),
                )
                .await
                .expect("independent manager shared a lock")
                .unwrap();
                drop(first);
                assert_eq!(left.prune().await, 1);
                assert_eq!(right.prune().await, 0);
                drop(second);
            }
        }
    };
}

lock_contract!(
    delivery,
    DeliveryLockManager,
    String,
    str::to_string,
    "DELIVERY_LOCK_CAPACITY"
);
lock_contract!(
    locator,
    LocatorLockManager,
    &str,
    borrowed_key,
    "LOCATOR_LOCK_CAPACITY"
);

#[tokio::test]
async fn delivery_and_locator_domains_remain_independent() {
    let delivery = DeliveryLockManager::new(1, Duration::ZERO);
    let locator = LocatorLockManager::new(1, Duration::ZERO);
    let first = delivery.acquire("same".to_string()).await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), locator.acquire("same"))
        .await
        .expect("delivery and locator lock domains were merged")
        .unwrap();
    drop(first);
    assert_eq!(delivery.prune().await, 1);
    assert_eq!(locator.prune().await, 0);
    drop(second);
}
