//! Turning a request into a person (HANDOFF 13.5). Handlers never read
//! headers for identity; they get the person from an [`Authenticator`].

use axum::http::HeaderMap;
use cork_core::{BoardError, ErrorCode, Result};
use subtle::ConstantTimeEq;

pub trait Authenticator: Send + Sync + 'static {
    /// The person the request acts for, or `unauthorized`.
    fn authenticate(&self, headers: &HeaderMap) -> Result<String>;
}

/// Portable mode: one token, from `daemon.json`, for the daemon's one person.
pub struct LocalToken {
    token: String,
    person: String,
}

impl LocalToken {
    pub fn new(token: impl Into<String>, person: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            person: person.into(),
        }
    }
}

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

impl Authenticator for LocalToken {
    fn authenticate(&self, headers: &HeaderMap) -> Result<String> {
        match bearer(headers) {
            Some(t) if bool::from(t.as_bytes().ct_eq(self.token.as_bytes())) => {
                Ok(self.person.clone())
            }
            _ => Err(BoardError::new(
                ErrorCode::Unauthorized,
                "missing or wrong token",
            )),
        }
    }
}
