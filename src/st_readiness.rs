use axum::http::StatusCode;

use crate::error::{AppError, AppResult};

pub const ST_WRITE_NOT_READY_CODE: &str = "ST_WRITE_NOT_READY";
pub const ST_WRITE_NOT_READY_MESSAGE: &str =
    "ST backend 未接入，聊天功能已冻结，本次未写入 SillyTavern。";

use std::sync::atomic::{AtomicU8, Ordering};

static READINESS: AtomicU8 = AtomicU8::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StWriteReadiness {
    pub backend_connected: bool,
    pub write_enabled: bool,
}

impl StWriteReadiness {
    pub const fn frozen() -> Self {
        Self {
            backend_connected: false,
            write_enabled: false,
        }
    }

    pub const fn can_write(self) -> bool {
        self.backend_connected && self.write_enabled
    }

    pub fn require_write(self) -> AppResult<()> {
        if self.can_write() {
            Ok(())
        } else {
            Err(AppError::new(
                ST_WRITE_NOT_READY_CODE,
                ST_WRITE_NOT_READY_MESSAGE,
                StatusCode::CONFLICT,
            ))
        }
    }
}

pub fn update(backend_connected: bool, write_enabled: bool) {
    let snapshot = backend_connected as u8 | (write_enabled as u8) << 1;
    READINESS.store(snapshot, Ordering::Relaxed);
}

pub fn current() -> StWriteReadiness {
    let snapshot = READINESS.load(Ordering::Relaxed);
    StWriteReadiness {
        backend_connected: snapshot & 1 != 0,
        write_enabled: snapshot & 2 != 0,
    }
}

pub fn require_write() -> AppResult<()> {
    current().require_write()
}

#[cfg(test)]
mod tests {
    use super::{StWriteReadiness, ST_WRITE_NOT_READY_CODE, ST_WRITE_NOT_READY_MESSAGE};

    #[test]
    fn default_readiness_is_fail_closed() {
        let readiness = StWriteReadiness::default();
        assert!(!readiness.can_write());
        let error = readiness
            .require_write()
            .expect_err("writes must be frozen");
        assert_eq!(error.code, ST_WRITE_NOT_READY_CODE);
        assert!(error.message.contains(ST_WRITE_NOT_READY_MESSAGE));
    }

    #[test]
    fn readiness_requires_both_backend_and_write_capability() {
        assert!(!StWriteReadiness {
            backend_connected: true,
            write_enabled: false,
        }
        .can_write());
        assert!(!StWriteReadiness {
            backend_connected: false,
            write_enabled: true,
        }
        .can_write());
        assert!(StWriteReadiness {
            backend_connected: true,
            write_enabled: true,
        }
        .can_write());
    }
}
