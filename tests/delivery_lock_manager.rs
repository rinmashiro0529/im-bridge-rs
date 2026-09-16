use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use im_bridge::modules::telegram::locks::DeliveryLockManager;
use tokio::sync::Barrier;
use tokio::time::timeout;

#[tokio::test]
async fn same_key_requests_are_strictly_serialized() {
    let manager = Arc::new(DeliveryLockManager::new(16, Duration::from_secs(60)));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(100);
    for _ in 0..100 {
        let manager = manager.clone();
        let active = active.clone();
        let peak = peak.clone();
        handles.push(tokio::spawn(async move {
            let _guard = manager
                .acquire("bot-a:1".into())
                .await
                .expect("same key must acquire");
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            tokio::task::yield_now().await;
            active.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for handle in handles {
        handle.await.expect("join serialized workers");
    }
    assert_eq!(peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn capacity_limit_enforced_and_evicts_oldest_idle_entry() {
    let manager = DeliveryLockManager::new(5, Duration::from_secs(60));
    for index in 0..5 {
        drop(
            manager
                .acquire(format!("idle-{index}"))
                .await
                .expect("seed idle lock"),
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(manager.len().await, 5);
    assert!(manager.contains("idle-0").await);
    manager
        .acquire("idle-5".into())
        .await
        .expect("sixth idle key must evict oldest");
    assert_eq!(manager.len().await, 5);
    assert!(!manager.contains("idle-0").await);
    assert!(manager.contains("idle-5").await);
}

#[tokio::test]
async fn capacity_exhausted_when_all_entries_active_returns_error() {
    let manager = Arc::new(DeliveryLockManager::new(2, Duration::from_secs(60)));
    let hold = Arc::new(Barrier::new(3));
    let first = {
        let manager = manager.clone();
        let hold = hold.clone();
        tokio::spawn(async move {
            let _guard = manager.acquire("active-a".into()).await.expect("hold a");
            hold.wait().await;
        })
    };
    let second = {
        let manager = manager.clone();
        let hold = hold.clone();
        tokio::spawn(async move {
            let _guard = manager.acquire("active-b".into()).await.expect("hold b");
            hold.wait().await;
        })
    };
    timeout(Duration::from_secs(1), async {
        loop {
            if manager.len().await == 2 && manager.active_count().await == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both active locks must be held");
    let error = timeout(
        Duration::from_millis(200),
        manager.acquire("active-c".into()),
    )
    .await
    .expect("capacity check must not wait on mutex")
    .expect_err("full active table must fail closed");
    assert_eq!(error, "DELIVERY_LOCK_CAPACITY");
    hold.wait().await;
    first.await.expect("release a");
    second.await.expect("release b");
}

#[tokio::test]
async fn waiter_cancellation_allows_eviction() {
    let manager = Arc::new(DeliveryLockManager::new(1, Duration::from_secs(60)));
    let hold = Arc::new(Barrier::new(2));
    let owner = {
        let manager = manager.clone();
        let hold = hold.clone();
        tokio::spawn(async move {
            let _guard = manager.acquire("held".into()).await.expect("owner lock");
            hold.wait().await;
        })
    };
    timeout(Duration::from_secs(1), async {
        loop {
            if manager.contains("held").await && manager.active_count().await == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner must hold the lock");
    let waiter = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.acquire("held".into()).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    waiter.abort();
    let aborted = waiter.await;
    assert!(aborted.is_err(), "waiter must be cancelled");
    hold.wait().await;
    owner.await.expect("owner released");
    drop(
        manager
            .acquire("replacement".into())
            .await
            .expect("cancelled waiter must not pin the idle lock"),
    );
    assert!(!manager.contains("held").await);
    assert!(manager.contains("replacement").await);
}

#[tokio::test]
async fn zero_capacity_fails_closed() {
    let manager = DeliveryLockManager::new(0, Duration::from_secs(60));
    let error = manager
        .acquire("any".into())
        .await
        .expect_err("zero capacity must fail closed");
    assert_eq!(error, "DELIVERY_LOCK_CAPACITY");
    assert_eq!(manager.len().await, 0);
}

#[tokio::test]
async fn prune_cleans_expired_idle_locks() {
    let manager = DeliveryLockManager::new(8, Duration::from_millis(20));
    drop(
        manager
            .acquire("stale".into())
            .await
            .expect("seed expired idle lock"),
    );
    let held = manager
        .acquire("active".into())
        .await
        .expect("keep an active lock");
    tokio::time::sleep(Duration::from_millis(40)).await;
    let removed = manager.prune().await;
    assert_eq!(removed, 1);
    assert!(!manager.contains("stale").await);
    assert!(manager.contains("active").await);
    drop(held);
}
