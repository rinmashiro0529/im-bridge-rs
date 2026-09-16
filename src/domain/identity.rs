use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceRole {
    Owner,
    Admin,
    Member,
}

impl WorkspaceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Admin => "admin",
            Self::Member => "member",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "admin" => Some(Self::Admin),
            "member" => Some(Self::Member),
            _ => None,
        }
    }

    pub fn can_admin(self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub is_system_admin: bool,
    pub disabled_at: Option<String>,
    pub legacy_st_handle: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Actor {
    pub account: Account,
    pub workspace_id: Option<String>,
    pub workspace_role: Option<WorkspaceRole>,
}

impl Actor {
    pub fn require_workspace(&self) -> Result<&str, crate::error::AppError> {
        self.workspace_id.as_deref().ok_or_else(|| {
            crate::error::AppError::bad_request("WORKSPACE_REQUIRED", "workspace is required")
        })
    }

    pub fn can_manage_workspace(&self) -> bool {
        self.account.is_system_admin
            || self
                .workspace_role
                .map(WorkspaceRole::can_admin)
                .unwrap_or(false)
    }
}
