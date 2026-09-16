use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use im_bridge::domain::st::{
    CommitStChat, CreateStChat, StCharacterSummary, StChatLocator, StChatSnapshot, StChatSummary,
    StCommitResult, StGenerationRequest, StGenerationResult, StGenerationSettings, StModelCatalog,
    StStatus,
};
use im_bridge::modules::bridge::errors::{
    CommitState, StBridgeError, StErrorCode, StErrorStage, StResult,
};
use im_bridge::seams::st_backend::StBackend;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq)]
pub enum StBackendCall {
    Probe,
    ListCharacters,
    ListChats { avatar: String },
    Snapshot { locator: StChatLocator },
    ListModels,
    GenerationSettings,
    StreamGenerate { request: Box<StGenerationRequest> },
    CreateChat { command: Box<CreateStChat> },
    Commit { command: Box<CommitStChat> },
}

pub struct ScriptedStBackend {
    probe: Mutex<VecDeque<StResult<StStatus>>>,
    characters: Mutex<VecDeque<StResult<Vec<StCharacterSummary>>>>,
    chats: Mutex<VecDeque<StResult<Vec<StChatSummary>>>>,
    snapshots: Mutex<VecDeque<StResult<StChatSnapshot>>>,
    models: Mutex<VecDeque<StResult<StModelCatalog>>>,
    settings: Mutex<VecDeque<StResult<StGenerationSettings>>>,
    generations: Mutex<VecDeque<StResult<StGenerationResult>>>,
    creates: Mutex<VecDeque<StResult<StCommitResult>>>,
    commits: Mutex<VecDeque<StResult<StCommitResult>>>,
    calls: Mutex<Vec<StBackendCall>>,
    generation_delay: Mutex<Option<Duration>>,
    generation_active: AtomicUsize,
    generation_peak: AtomicUsize,
    snapshot_delay: Mutex<Option<Duration>>,
    snapshot_active: AtomicUsize,
    snapshot_peak: AtomicUsize,
}

impl Default for ScriptedStBackend {
    fn default() -> Self {
        Self {
            probe: Mutex::new(VecDeque::new()),
            characters: Mutex::new(VecDeque::new()),
            chats: Mutex::new(VecDeque::new()),
            snapshots: Mutex::new(VecDeque::new()),
            models: Mutex::new(VecDeque::new()),
            settings: Mutex::new(VecDeque::new()),
            generations: Mutex::new(VecDeque::new()),
            creates: Mutex::new(VecDeque::new()),
            commits: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
            generation_delay: Mutex::new(None),
            generation_active: AtomicUsize::new(0),
            generation_peak: AtomicUsize::new(0),
            snapshot_delay: Mutex::new(None),
            snapshot_active: AtomicUsize::new(0),
            snapshot_peak: AtomicUsize::new(0),
        }
    }
}

impl ScriptedStBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_probe(&self, result: StResult<StStatus>) {
        self.probe.lock().expect("script lock").push_back(result);
    }

    pub fn push_characters(&self, result: StResult<Vec<StCharacterSummary>>) {
        self.characters
            .lock()
            .expect("script lock")
            .push_back(result);
    }

    pub fn push_chats(&self, result: StResult<Vec<StChatSummary>>) {
        self.chats.lock().expect("script lock").push_back(result);
    }

    pub fn push_snapshot(&self, result: StResult<StChatSnapshot>) {
        self.snapshots
            .lock()
            .expect("script lock")
            .push_back(result);
    }

    pub fn push_models(&self, result: StResult<StModelCatalog>) {
        self.models.lock().expect("script lock").push_back(result);
    }

    pub fn push_settings(&self, result: StResult<StGenerationSettings>) {
        self.settings.lock().expect("script lock").push_back(result);
    }

    pub fn push_generation(&self, result: StResult<StGenerationResult>) {
        self.generations
            .lock()
            .expect("script lock")
            .push_back(result);
    }

    pub fn push_create(&self, result: StResult<StCommitResult>) {
        self.creates.lock().expect("script lock").push_back(result);
    }

    pub fn push_commit(&self, result: StResult<StCommitResult>) {
        self.commits.lock().expect("script lock").push_back(result);
    }

    pub fn calls(&self) -> Vec<StBackendCall> {
        self.calls.lock().expect("call lock").clone()
    }

    pub fn set_generation_delay(&self, delay: Duration) {
        *self.generation_delay.lock().expect("script lock") = Some(delay);
    }

    pub fn generation_peak(&self) -> usize {
        self.generation_peak.load(Ordering::SeqCst)
    }

    pub fn set_snapshot_delay(&self, delay: Duration) {
        *self.snapshot_delay.lock().expect("script lock") = Some(delay);
    }

    pub fn snapshot_peak(&self) -> usize {
        self.snapshot_peak.load(Ordering::SeqCst)
    }

    fn record(&self, call: StBackendCall) {
        self.calls.lock().expect("call lock").push(call);
    }

    fn missing_script<T>(name: &'static str) -> StResult<T> {
        Err(StBridgeError::boxed(
            StErrorCode::StTestScopeRequired,
            StErrorStage::Control,
            format!("no scripted ST result for {name}"),
            false,
            CommitState::NotStarted,
        ))
    }
}

#[async_trait]
impl StBackend for ScriptedStBackend {
    async fn probe(&self) -> StResult<StStatus> {
        self.record(StBackendCall::Probe);
        self.probe
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("probe"))
    }

    async fn list_characters(&self) -> StResult<Vec<StCharacterSummary>> {
        self.record(StBackendCall::ListCharacters);
        self.characters
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("list_characters"))
    }

    async fn list_chats(&self, avatar: &str) -> StResult<Vec<StChatSummary>> {
        self.record(StBackendCall::ListChats {
            avatar: avatar.to_string(),
        });
        self.chats
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("list_chats"))
    }

    async fn snapshot(&self, locator: &StChatLocator) -> StResult<StChatSnapshot> {
        self.record(StBackendCall::Snapshot {
            locator: locator.clone(),
        });
        let delay = *self.snapshot_delay.lock().expect("script lock");
        let active = self.snapshot_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.snapshot_peak.fetch_max(active, Ordering::SeqCst);
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.snapshot_active.fetch_sub(1, Ordering::SeqCst);
        self.snapshots
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("snapshot"))
    }

    async fn list_models(&self) -> StResult<StModelCatalog> {
        self.record(StBackendCall::ListModels);
        self.models
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("list_models"))
    }

    async fn generation_settings(&self) -> StResult<StGenerationSettings> {
        self.record(StBackendCall::GenerationSettings);
        self.settings
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("generation_settings"))
    }

    async fn stream_generate(
        &self,
        request: StGenerationRequest,
        _progress: Option<
            std::sync::Arc<dyn im_bridge::seams::bridge_progress::BridgeProgressSink>,
        >,
        _cancel: CancellationToken,
    ) -> StResult<StGenerationResult> {
        self.record(StBackendCall::StreamGenerate {
            request: Box::new(request),
        });
        let delay = *self.generation_delay.lock().expect("script lock");
        let active = self.generation_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.generation_peak.fetch_max(active, Ordering::SeqCst);
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.generation_active.fetch_sub(1, Ordering::SeqCst);
        self.generations
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("stream_generate"))
    }

    async fn create_chat(&self, command: CreateStChat) -> StResult<StCommitResult> {
        self.record(StBackendCall::CreateChat {
            command: Box::new(command),
        });
        self.creates
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("create_chat"))
    }

    async fn commit(&self, command: CommitStChat) -> StResult<StCommitResult> {
        self.record(StBackendCall::Commit {
            command: Box::new(command),
        });
        self.commits
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("commit"))
    }
}
