//! The Cork daemon library: the board engine, the HTTP API and the lease
//! timer. The binary (`cork serve`) wires a store, a clock and an
//! authenticator into [`serve`].
//!
//! The server has no SQLite types or SQL: it only sees the `Store` trait
//! (HANDOFF 13.5).

pub mod auth;
pub mod board;
pub mod clock;
pub mod http;
pub mod metrics;

pub use auth::{Authenticator, LocalToken};

/// HTTP types an [`Authenticator`] sees, re-exported so callers need no axum.
pub mod http_types {
    pub use axum::http::HeaderMap;
}
pub use board::{Board, BoardConfig, Caller};
pub use clock::{Clock, ManualClock, SystemClock};
pub use http::{App, HostPolicy, router};

use cork_store::Store;
use std::sync::Arc;

/// Serve the API on `listener` until `shutdown` resolves. In-flight requests
/// get up to 10 seconds to finish (13.4).
pub async fn serve<S: Store>(
    listener: tokio::net::TcpListener,
    app: Arc<App<S>>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, router(app)).with_graceful_shutdown(async move {
        shutdown.await;
        let _ = tx.send(());
    });
    tokio::select! {
        r = server => r,
        _ = async {
            let _ = rx.await;
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        } => Ok(()),
    }
}
