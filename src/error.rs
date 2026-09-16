use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
#[error("{code}: {message}")]
pub struct AppError {
    pub code: &'static str,
    pub message: String,
    pub status: StatusCode,
    pub st: Option<Box<crate::modules::bridge::errors::StBridgeError>>,
}

impl AppError {
    pub fn new(code: &'static str, message: impl Into<String>, status: StatusCode) -> Self {
        Self {
            code,
            message: message.into(),
            status,
            st: None,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new("UNAUTHORIZED", message, StatusCode::UNAUTHORIZED)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new("FORBIDDEN", message, StatusCode::FORBIDDEN)
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message, StatusCode::NOT_FOUND)
    }

    pub fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message, StatusCode::BAD_REQUEST)
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message, StatusCode::CONFLICT)
    }

    pub fn gone(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message, StatusCode::GONE)
    }

    pub fn too_many(message: impl Into<String>) -> Self {
        Self::new("RATE_LIMITED", message, StatusCode::TOO_MANY_REQUESTS)
    }

    pub fn service_unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message, StatusCode::SERVICE_UNAVAILABLE)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new("INTERNAL_ERROR", message, StatusCode::INTERNAL_SERVER_ERROR)
    }

    pub fn bad_gateway(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(code, message, StatusCode::BAD_GATEWAY)
    }

    pub fn from_st(error: impl Into<Box<crate::modules::bridge::errors::StBridgeError>>) -> Self {
        let error = error.into();
        let status = match error.code {
            crate::modules::bridge::errors::StErrorCode::StWriteNotReady => StatusCode::CONFLICT,
            crate::modules::bridge::errors::StErrorCode::StChatConflict => StatusCode::CONFLICT,
            crate::modules::bridge::errors::StErrorCode::StChatNotFound => StatusCode::NOT_FOUND,
            crate::modules::bridge::errors::StErrorCode::StGenerateRateLimited => {
                StatusCode::TOO_MANY_REQUESTS
            }
            crate::modules::bridge::errors::StErrorCode::StSessionRejected
            | crate::modules::bridge::errors::StErrorCode::StCsrfMissing
            | crate::modules::bridge::errors::StErrorCode::StConnectorAuthFailed => {
                StatusCode::FORBIDDEN
            }
            _ if error.retryable => StatusCode::BAD_GATEWAY,
            _ => StatusCode::BAD_REQUEST,
        };
        Self {
            code: error.code.as_str(),
            message: error.safe_message.clone(),
            status,
            st: Some(error),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: &'a str,
    #[serde(rename = "requestId", skip_serializing_if = "Option::is_none")]
    request_id: Option<&'a str>,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = Json(ErrorBody {
            error: ErrorDetail {
                code: self.code,
                message: &self.message,
                request_id: None,
            },
        });
        (self.status, body).into_response()
    }
}

impl From<sqlx::Error> for AppError {
    fn from(value: sqlx::Error) -> Self {
        tracing::error!(error = %value, "database error");
        AppError::internal("database error")
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        tracing::error!(error = %value, "io error");
        AppError::internal("io error")
    }
}

pub type AppResult<T> = Result<T, AppError>;
