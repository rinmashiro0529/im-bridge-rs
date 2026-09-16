use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub actor_id: Option<String>,
    pub operation: String,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub result: String,
    pub metadata: Value,
}
