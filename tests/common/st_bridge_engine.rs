use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;

use im_bridge::domain::identity::Actor;
use im_bridge::modules::bridge::errors::{
    CommitState, StBridgeError, StErrorCode, StErrorStage, StResult,
};
use im_bridge::seams::bridge_progress::BridgeExecutionContext;
use im_bridge::seams::st_bridge_engine::{
    BridgeOperationOrigin, StBridgeCommand, StBridgeEngine, StBridgeOutcome, StBridgeQuery,
    StBridgeView,
};

#[derive(Debug, Clone, PartialEq)]
pub enum StBridgeEngineCall {
    Execute { command: Box<StBridgeCommand> },
    Query { query: StBridgeQuery },
}

pub struct ScriptedStBridgeEngine {
    executes: Mutex<VecDeque<StResult<StBridgeOutcome>>>,
    queries: Mutex<VecDeque<StResult<StBridgeView>>>,
    calls: Mutex<Vec<StBridgeEngineCall>>,
}

impl Default for ScriptedStBridgeEngine {
    fn default() -> Self {
        Self {
            executes: Mutex::new(VecDeque::new()),
            queries: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl ScriptedStBridgeEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_execute(&self, result: StResult<StBridgeOutcome>) {
        self.executes.lock().expect("script lock").push_back(result);
    }

    pub fn push_query(&self, result: StResult<StBridgeView>) {
        self.queries.lock().expect("script lock").push_back(result);
    }

    pub fn calls(&self) -> Vec<StBridgeEngineCall> {
        self.calls.lock().expect("call lock").clone()
    }

    fn record(&self, call: StBridgeEngineCall) {
        self.calls.lock().expect("call lock").push(call);
    }

    fn missing_script<T>(name: &'static str) -> StResult<T> {
        Err(StBridgeError::boxed(
            StErrorCode::StTestScopeRequired,
            StErrorStage::Control,
            format!("no scripted ST bridge result for {name}"),
            false,
            CommitState::NotStarted,
        ))
    }
}

#[async_trait]
impl StBridgeEngine for ScriptedStBridgeEngine {
    async fn execute(&self, _actor: &Actor, command: StBridgeCommand) -> StResult<StBridgeOutcome> {
        self.record(StBridgeEngineCall::Execute {
            command: Box::new(command),
        });
        self.executes
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("execute"))
    }

    async fn execute_with_origin(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        _origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        self.execute(actor, command).await
    }

    async fn execute_with_context_and_origin(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        _context: Option<BridgeExecutionContext>,
        _origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        self.execute(actor, command).await
    }

    async fn query(&self, _actor: &Actor, query: StBridgeQuery) -> StResult<StBridgeView> {
        self.record(StBridgeEngineCall::Query { query });
        self.queries
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| Self::missing_script("query"))
    }
}
