use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock as StdRwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollerOwner {
    LegacyPlugin,
    RustBridge,
}

impl PollerOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyPlugin => "legacy_plugin",
            Self::RustBridge => "rust_bridge",
        }
    }
}

impl fmt::Display for PollerOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollerOwnershipRecord {
    pub telegram_bot_id: i64,
    pub owner: PollerOwner,
    pub epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollerError {
    BootstrapClosed,
    NotRegistered,
    NotOwner,
    EpochMismatch,
    InvalidBotId,
    RegistryUnavailable,
    Conflict,
    LifecycleInvalid,
}

impl PollerError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BootstrapClosed => "poller bootstrap closed",
            Self::NotRegistered => "poller not registered for numeric bot id",
            Self::NotOwner => "poller not owner for numeric bot id",
            Self::EpochMismatch => "poller epoch mismatch for numeric bot id",
            Self::InvalidBotId => "invalid numeric bot id",
            Self::RegistryUnavailable => "poller ownership registry unavailable",
            Self::Conflict => "poller ownership conflict",
            Self::LifecycleInvalid => "poller runtime lifecycle is not writable",
        }
    }
}

impl fmt::Display for PollerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for PollerError {}

fn require_numeric_bot_id(telegram_bot_id: i64) -> Result<(), PollerError> {
    if telegram_bot_id <= 0 {
        Err(PollerError::InvalidBotId)
    } else {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryPollerRegistry {
    records: HashMap<i64, PollerOwnershipRecord>,
    runtime_bindings: HashMap<i64, PollerRuntimeBinding>,
    bootstrapped: bool,
}

impl MemoryPollerRegistry {
    /// 仅当 registry 空且尚未 bootstrap 时允许一次性写入 owner=LegacyPlugin, epoch=1。
    /// 之后永久拒绝 auto claim。
    pub fn bootstrap_legacy(
        &mut self,
        telegram_bot_id: i64,
    ) -> Result<PollerOwnershipRecord, PollerError> {
        require_numeric_bot_id(telegram_bot_id)?;
        if self.bootstrapped || !self.records.is_empty() {
            return Err(PollerError::BootstrapClosed);
        }
        let record = PollerOwnershipRecord {
            telegram_bot_id,
            owner: PollerOwner::LegacyPlugin,
            epoch: 1,
        };
        self.records.insert(telegram_bot_id, record.clone());
        self.bootstrapped = true;
        Ok(record)
    }

    /// 不改变 owner。只检查。
    pub fn heartbeat(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        epoch: u64,
    ) -> Result<(), PollerError> {
        self.assert_can_start(telegram_bot_id, owner, epoch)
    }

    pub fn transfer(
        &mut self,
        telegram_bot_id: i64,
        expected_owner: PollerOwner,
        expected_epoch: u64,
        new_owner: PollerOwner,
    ) -> Result<PollerOwnershipRecord, PollerError> {
        require_numeric_bot_id(telegram_bot_id)?;
        let record = self
            .records
            .get(&telegram_bot_id)
            .ok_or(PollerError::NotRegistered)?;
        if record.owner != expected_owner {
            return Err(PollerError::NotOwner);
        }
        if record.epoch != expected_epoch {
            return Err(PollerError::EpochMismatch);
        }
        if new_owner == record.owner {
            return Err(PollerError::NotOwner);
        }
        let epoch = record.epoch.checked_add(1).ok_or(PollerError::Conflict)?;
        let updated = PollerOwnershipRecord {
            telegram_bot_id,
            owner: new_owner,
            epoch,
        };
        self.records.insert(telegram_bot_id, updated.clone());
        Ok(updated)
    }

    pub fn assert_can_start(
        &self,
        telegram_bot_id: i64,
        runtime_owner: PollerOwner,
        runtime_epoch: u64,
    ) -> Result<(), PollerError> {
        require_numeric_bot_id(telegram_bot_id)?;
        let record = self
            .records
            .get(&telegram_bot_id)
            .ok_or(PollerError::NotRegistered)?;
        if record.owner != runtime_owner {
            return Err(PollerError::NotOwner);
        }
        if record.epoch != runtime_epoch {
            return Err(PollerError::EpochMismatch);
        }
        Ok(())
    }

    pub fn claim_runtime(
        &mut self,
        telegram_bot_id: i64,
        binding: PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        require_numeric_bot_id(telegram_bot_id)?;
        if !binding.lifecycle.can_own()
            || binding.internal_bot_id.trim().is_empty()
            || binding.runtime_instance_id.trim().is_empty()
        {
            return Err(PollerError::LifecycleInvalid);
        }
        self.assert_can_start(telegram_bot_id, PollerOwner::RustBridge, binding.epoch)?;
        if self.runtime_bindings.contains_key(&telegram_bot_id) {
            return Err(PollerError::Conflict);
        }
        self.runtime_bindings.insert(telegram_bot_id, binding);
        Ok(())
    }

    pub fn release_runtime(
        &mut self,
        telegram_bot_id: i64,
        runtime_instance_id: &str,
    ) -> Result<(), PollerError> {
        require_numeric_bot_id(telegram_bot_id)?;
        let Some(binding) = self.runtime_bindings.get(&telegram_bot_id) else {
            return Err(PollerError::NotRegistered);
        };
        if binding.runtime_instance_id != runtime_instance_id {
            return Err(PollerError::NotOwner);
        }
        self.runtime_bindings.remove(&telegram_bot_id);
        Ok(())
    }

    pub fn runtime_binding(&self, telegram_bot_id: i64) -> Option<&PollerRuntimeBinding> {
        self.runtime_bindings.get(&telegram_bot_id)
    }

    pub fn get(&self, telegram_bot_id: i64) -> Option<&PollerOwnershipRecord> {
        self.records.get(&telegram_bot_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollerLifecycle {
    Starting,
    Running,
    Draining,
    Stopped,
}

impl PollerLifecycle {
    pub const fn can_own(self) -> bool {
        matches!(self, Self::Starting | Self::Running)
    }

    pub const fn accepts_writes(self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Runtime identity bound to one numeric Telegram bot ownership epoch.
///
/// `PollerOwnershipRecord` remains the wire-compatible server owner record;
/// this binding is the local hard fence carried by every poller operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollerRuntimeBinding {
    pub internal_bot_id: String,
    pub runtime_instance_id: String,
    pub epoch: u64,
    pub lifecycle: PollerLifecycle,
}

impl PollerRuntimeBinding {
    pub fn new(
        internal_bot_id: impl Into<String>,
        runtime_instance_id: impl Into<String>,
        epoch: u64,
    ) -> Self {
        Self {
            internal_bot_id: internal_bot_id.into(),
            runtime_instance_id: runtime_instance_id.into(),
            epoch,
            lifecycle: PollerLifecycle::Starting,
        }
    }

    pub fn running(mut self) -> Self {
        self.lifecycle = PollerLifecycle::Running;
        self
    }

    pub fn draining(mut self) -> Self {
        self.lifecycle = PollerLifecycle::Draining;
        self
    }
}

#[async_trait::async_trait]
pub trait PollerOwnershipRegistry: Send + Sync {
    async fn get_ownership(
        &self,
        telegram_bot_id: i64,
    ) -> Result<Option<PollerOwnershipRecord>, PollerError>;
    async fn assert_can_start(
        &self,
        telegram_bot_id: i64,
        runtime_owner: PollerOwner,
        runtime_epoch: u64,
    ) -> Result<(), PollerError>;
    async fn heartbeat(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        epoch: u64,
    ) -> Result<(), PollerError>;

    /// Validate the complete runtime fence without adopting a newer server
    /// epoch.  Implementations may add an atomic server-side binding check;
    /// the default keeps compatibility with the legacy ownership registry.
    async fn assert_fence(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        binding: &PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        if binding.internal_bot_id.trim().is_empty()
            || binding.runtime_instance_id.trim().is_empty()
        {
            return Err(PollerError::Conflict);
        }
        if binding.epoch == 0 || !binding.lifecycle.accepts_writes() {
            return Err(PollerError::LifecycleInvalid);
        }
        self.assert_can_start(telegram_bot_id, owner, binding.epoch)
            .await
    }

    async fn claim_runtime(
        &self,
        telegram_bot_id: i64,
        binding: PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        if binding.internal_bot_id.trim().is_empty()
            || binding.runtime_instance_id.trim().is_empty()
            || binding.epoch == 0
            || !binding.lifecycle.can_own()
        {
            return Err(PollerError::LifecycleInvalid);
        }
        self.assert_can_start(telegram_bot_id, PollerOwner::RustBridge, binding.epoch)
            .await
    }

    async fn release_runtime(
        &self,
        _telegram_bot_id: i64,
        _runtime_instance_id: &str,
    ) -> Result<(), PollerError> {
        Err(PollerError::RegistryUnavailable)
    }
}

#[derive(Clone, Default)]
pub struct SharedMemoryPollerRegistry {
    inner: Arc<RwLock<MemoryPollerRegistry>>,
}

impl SharedMemoryPollerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn bootstrap_legacy(
        &self,
        telegram_bot_id: i64,
    ) -> Result<PollerOwnershipRecord, PollerError> {
        self.inner.write().await.bootstrap_legacy(telegram_bot_id)
    }

    pub async fn claim_runtime(
        &self,
        telegram_bot_id: i64,
        binding: PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        self.inner
            .write()
            .await
            .claim_runtime(telegram_bot_id, binding)
    }

    pub async fn release_runtime(
        &self,
        telegram_bot_id: i64,
        runtime_instance_id: &str,
    ) -> Result<(), PollerError> {
        self.inner
            .write()
            .await
            .release_runtime(telegram_bot_id, runtime_instance_id)
    }

    pub async fn runtime_binding(&self, telegram_bot_id: i64) -> Option<PollerRuntimeBinding> {
        self.inner
            .read()
            .await
            .runtime_binding(telegram_bot_id)
            .cloned()
    }

    pub async fn transfer(
        &self,
        telegram_bot_id: i64,
        expected_owner: PollerOwner,
        expected_epoch: u64,
        new_owner: PollerOwner,
    ) -> Result<PollerOwnershipRecord, PollerError> {
        self.inner.write().await.transfer(
            telegram_bot_id,
            expected_owner,
            expected_epoch,
            new_owner,
        )
    }
}

#[async_trait::async_trait]
impl PollerOwnershipRegistry for SharedMemoryPollerRegistry {
    async fn get_ownership(
        &self,
        telegram_bot_id: i64,
    ) -> Result<Option<PollerOwnershipRecord>, PollerError> {
        require_numeric_bot_id(telegram_bot_id)?;
        Ok(self.inner.read().await.get(telegram_bot_id).cloned())
    }

    async fn assert_can_start(
        &self,
        telegram_bot_id: i64,
        runtime_owner: PollerOwner,
        runtime_epoch: u64,
    ) -> Result<(), PollerError> {
        self.inner
            .read()
            .await
            .assert_can_start(telegram_bot_id, runtime_owner, runtime_epoch)
    }

    async fn assert_fence(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        binding: &PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        if !binding.lifecycle.accepts_writes() {
            return Err(PollerError::LifecycleInvalid);
        }
        let registry = self.inner.read().await;
        registry.assert_can_start(telegram_bot_id, owner, binding.epoch)?;
        let current = registry
            .runtime_binding(telegram_bot_id)
            .ok_or(PollerError::NotRegistered)?;
        if current.internal_bot_id != binding.internal_bot_id
            || current.runtime_instance_id != binding.runtime_instance_id
        {
            return Err(PollerError::Conflict);
        }
        if current.epoch != binding.epoch {
            return Err(PollerError::EpochMismatch);
        }
        Ok(())
    }

    async fn heartbeat(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        epoch: u64,
    ) -> Result<(), PollerError> {
        self.inner
            .read()
            .await
            .heartbeat(telegram_bot_id, owner, epoch)
    }

    async fn claim_runtime(
        &self,
        telegram_bot_id: i64,
        binding: PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        self.inner
            .write()
            .await
            .claim_runtime(telegram_bot_id, binding)
    }

    async fn release_runtime(
        &self,
        telegram_bot_id: i64,
        runtime_instance_id: &str,
    ) -> Result<(), PollerError> {
        self.inner
            .write()
            .await
            .release_runtime(telegram_bot_id, runtime_instance_id)
    }
}

#[derive(Clone)]
pub struct PollerOwnershipGuard {
    pub numeric_bot_id: i64,
    pub owner: PollerOwner,
    pub epoch: u64,
    pub internal_bot_id: String,
    pub runtime_instance_id: String,
    pub lifecycle: PollerLifecycle,
    lifecycle_state: Arc<StdRwLock<PollerLifecycle>>,
    pub registry: Arc<dyn PollerOwnershipRegistry>,
}

impl PollerOwnershipGuard {
    pub fn new(
        numeric_bot_id: i64,
        owner: PollerOwner,
        epoch: u64,
        registry: Arc<dyn PollerOwnershipRegistry>,
    ) -> Self {
        Self {
            numeric_bot_id,
            owner,
            epoch,
            internal_bot_id: String::new(),
            runtime_instance_id: uuid::Uuid::new_v4().to_string(),
            lifecycle: PollerLifecycle::Starting,
            lifecycle_state: Arc::new(StdRwLock::new(PollerLifecycle::Starting)),
            registry,
        }
    }

    pub fn new_with_binding(
        numeric_bot_id: i64,
        owner: PollerOwner,
        binding: PollerRuntimeBinding,
        registry: Arc<dyn PollerOwnershipRegistry>,
    ) -> Self {
        Self {
            numeric_bot_id,
            owner,
            epoch: binding.epoch,
            internal_bot_id: binding.internal_bot_id,
            runtime_instance_id: binding.runtime_instance_id,
            lifecycle: binding.lifecycle,
            lifecycle_state: Arc::new(StdRwLock::new(binding.lifecycle)),
            registry,
        }
    }

    pub async fn claim_runtime(&self) -> Result<(), PollerError> {
        self.registry
            .claim_runtime(self.numeric_bot_id, self.binding(""))
            .await
    }

    pub async fn release_runtime(&self) -> Result<(), PollerError> {
        if !matches!(
            self.current_lifecycle(),
            PollerLifecycle::Draining | PollerLifecycle::Stopped
        ) {
            return Err(PollerError::LifecycleInvalid);
        }
        self.registry
            .release_runtime(self.numeric_bot_id, &self.runtime_instance_id)
            .await
    }

    pub fn runtime_fence(&self) -> Option<crate::domain::st::PollerRuntimeFence> {
        if self.owner != PollerOwner::RustBridge
            || self.current_lifecycle() != PollerLifecycle::Running
            || self.numeric_bot_id <= 0
            || self.epoch == 0
            || self.internal_bot_id.trim().is_empty()
            || self.runtime_instance_id.trim().is_empty()
        {
            return None;
        }
        Some(crate::domain::st::PollerRuntimeFence {
            telegram_bot_id: self.numeric_bot_id,
            owner: self.owner.as_str().to_string(),
            epoch: self.epoch,
            internal_bot_id: self.internal_bot_id.clone(),
            runtime_instance_id: self.runtime_instance_id.clone(),
        })
    }

    pub fn binding(&self, internal_bot_id: impl Into<String>) -> PollerRuntimeBinding {
        let supplied = internal_bot_id.into();
        PollerRuntimeBinding {
            internal_bot_id: if supplied.trim().is_empty() {
                self.internal_bot_id.clone()
            } else {
                supplied
            },
            runtime_instance_id: self.runtime_instance_id.clone(),
            epoch: self.epoch,
            lifecycle: self.current_lifecycle(),
        }
    }

    fn current_lifecycle(&self) -> PollerLifecycle {
        self.lifecycle_state
            .read()
            .map(|lifecycle| *lifecycle)
            .unwrap_or(self.lifecycle)
    }

    pub fn set_lifecycle_shared(&self, lifecycle: PollerLifecycle) -> Result<(), PollerError> {
        let mut current = self
            .lifecycle_state
            .write()
            .map_err(|_| PollerError::LifecycleInvalid)?;
        *current = lifecycle;
        Ok(())
    }

    pub fn set_lifecycle(&mut self, lifecycle: PollerLifecycle) -> Result<(), PollerError> {
        self.lifecycle = lifecycle;
        self.set_lifecycle_shared(lifecycle)
    }

    pub fn begin_draining(&self) -> Result<(), PollerError> {
        let mut lifecycle = self
            .lifecycle_state
            .write()
            .map_err(|_| PollerError::LifecycleInvalid)?;
        match *lifecycle {
            PollerLifecycle::Starting | PollerLifecycle::Running => {
                *lifecycle = PollerLifecycle::Draining;
                Ok(())
            }
            PollerLifecycle::Draining => Ok(()),
            PollerLifecycle::Stopped => Err(PollerError::LifecycleInvalid),
        }
    }

    pub fn mark_stopped(&self) -> Result<(), PollerError> {
        let mut lifecycle = self
            .lifecycle_state
            .write()
            .map_err(|_| PollerError::LifecycleInvalid)?;
        if matches!(
            *lifecycle,
            PollerLifecycle::Draining | PollerLifecycle::Stopped
        ) {
            *lifecycle = PollerLifecycle::Stopped;
            Ok(())
        } else {
            Err(PollerError::LifecycleInvalid)
        }
    }

    pub async fn assert_valid(&self) -> Result<(), PollerError> {
        self.registry
            .assert_fence(self.numeric_bot_id, self.owner, &self.binding(""))
            .await
    }

    pub async fn assert_generation_fence(&self) -> Result<(), PollerError> {
        self.assert_valid().await
    }

    pub async fn assert_commit_fence(&self) -> Result<(), PollerError> {
        self.assert_valid().await
    }

    pub async fn heartbeat(&self) -> Result<(), PollerError> {
        self.registry
            .heartbeat(self.numeric_bot_id, self.owner, self.epoch)
            .await
    }
}
