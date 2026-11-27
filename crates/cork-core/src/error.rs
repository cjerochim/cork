use serde::{Deserialize, Serialize};
use std::fmt;

/// Stable error codes. Clients match on these strings; never rename one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Another worker holds a live claim on the task.
    TaskClaimed,
    /// The claim id is not the task's current live claim (expired or replaced).
    ClaimLost,
    /// The caller has no claim on the task.
    ClaimNotHeld,
    /// `claim --ready` found nothing to claim.
    NothingReady,
    /// A wait ended before anything matched.
    Timeout,
    /// A user tried to set a label under `board.`.
    ReservedLabel,
    /// Only the task owner may do this.
    TaskNotOwned,
    /// The task is not in a state that allows this change.
    InvalidState,
    NotFound,
    InvalidInput,
    Unauthorized,
    Forbidden,
    /// The idempotency key was used before with a different request.
    IdempotencyMismatch,
    /// Reserved: the position is older than the kept log.
    CursorExpired,
    RateLimited,
    /// A bug, including a failed version guard in the store.
    Internal,
    /// The store cannot write (for example, disk full) or the server is at a limit.
    Unavailable,
}

impl ErrorCode {
    pub const ALL: [ErrorCode; 17] = [
        ErrorCode::TaskClaimed,
        ErrorCode::ClaimLost,
        ErrorCode::ClaimNotHeld,
        ErrorCode::NothingReady,
        ErrorCode::Timeout,
        ErrorCode::ReservedLabel,
        ErrorCode::TaskNotOwned,
        ErrorCode::InvalidState,
        ErrorCode::NotFound,
        ErrorCode::InvalidInput,
        ErrorCode::Unauthorized,
        ErrorCode::Forbidden,
        ErrorCode::IdempotencyMismatch,
        ErrorCode::CursorExpired,
        ErrorCode::RateLimited,
        ErrorCode::Internal,
        ErrorCode::Unavailable,
    ];

    /// The HTTP status for this code (HANDOFF 7.3). `nothing_ready` and
    /// `timeout` are claim outcomes, sent as 200, not errors.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorCode::TaskClaimed
            | ErrorCode::ClaimLost
            | ErrorCode::ClaimNotHeld
            | ErrorCode::InvalidState
            | ErrorCode::IdempotencyMismatch => 409,
            ErrorCode::TaskNotOwned | ErrorCode::Forbidden => 403,
            ErrorCode::NotFound => 404,
            ErrorCode::CursorExpired => 410,
            ErrorCode::ReservedLabel | ErrorCode::InvalidInput => 400,
            ErrorCode::Unauthorized => 401,
            ErrorCode::RateLimited => 429,
            ErrorCode::Internal => 500,
            ErrorCode::Unavailable => 503,
            ErrorCode::NothingReady | ErrorCode::Timeout => 200,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::TaskClaimed => "task_claimed",
            ErrorCode::ClaimLost => "claim_lost",
            ErrorCode::ClaimNotHeld => "claim_not_held",
            ErrorCode::NothingReady => "nothing_ready",
            ErrorCode::Timeout => "timeout",
            ErrorCode::ReservedLabel => "reserved_label",
            ErrorCode::TaskNotOwned => "task_not_owned",
            ErrorCode::InvalidState => "invalid_state",
            ErrorCode::NotFound => "not_found",
            ErrorCode::InvalidInput => "invalid_input",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::IdempotencyMismatch => "idempotency_mismatch",
            ErrorCode::CursorExpired => "cursor_expired",
            ErrorCode::RateLimited => "rate_limited",
            ErrorCode::Internal => "internal",
            ErrorCode::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardError {
    pub code: ErrorCode,
    pub message: String,
}

impl BoardError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidInput, message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

impl fmt::Display for BoardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for BoardError {}

pub type Result<T, E = BoardError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_stable_strings() {
        for c in ErrorCode::ALL {
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(json, format!("\"{}\"", c.as_str()));
        }
        assert_eq!(ErrorCode::ClaimLost.as_str(), "claim_lost");
        assert_eq!(ErrorCode::ClaimLost.http_status(), 409);
        assert_eq!(ErrorCode::Timeout.http_status(), 200);
    }
}
