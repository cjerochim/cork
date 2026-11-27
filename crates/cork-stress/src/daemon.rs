//! A daemon in this process, on a loopback port, over a fresh SQLite file.

use crate::client::Client;
use cork_server::*;
use cork_store_sqlite::{Durability, SqliteConfig, SqliteStore};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Sync {
    Full,
    Normal,
}

pub struct Daemon {
    pub url: String,
    pub token: String,
    pub path: PathBuf,
    pub board: Arc<Board<SqliteStore>>,
    http: reqwest::Client,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    _dir: tempfile::TempDir,
}

const TOKEN: &str = "stress-token";

impl Daemon {
    pub async fn start(sync: Sync) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("cork.db");
        Self::open(dir, path, sync).await
    }

    async fn open(dir: tempfile::TempDir, path: PathBuf, sync: Sync) -> Self {
        raise_fd_limit();
        let mut cfg = SqliteConfig::new(&path);
        cfg.durability = match sync {
            Sync::Full => Durability::Full,
            Sync::Normal => Durability::Normal,
        };
        let store = Arc::new(SqliteStore::open(cfg).await.expect("open store"));
        let floor = cork_store::Store::max_at(&*store).await.expect("max_at");
        let mut wal = path.clone().into_os_string();
        wal.push("-wal");
        let board = Board::start(
            store,
            Arc::new(SystemClock::anchored(floor)),
            BoardConfig {
                wal_path: Some(wal.into()),
                ..Default::default()
            },
        )
        .await
        .expect("start board");
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let app = Arc::new(App {
            board: board.clone(),
            auth: Arc::new(LocalToken::new(TOKEN, "stress")),
            hosts: HostPolicy::loopback(port),
        });
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = serve(listener, app, async {
                let _ = rx.await;
            })
            .await;
        });
        Self {
            url: format!("http://127.0.0.1:{port}/v1"),
            token: TOKEN.into(),
            path,
            board,
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(2_000)
                .build()
                .expect("http client"),
            stop: Some(tx),
            _dir: dir,
        }
    }

    pub fn client(&self, profile: &str) -> Client {
        Client::new(&self.url, &self.token, profile, self.http.clone())
    }

    /// Stop, then open the same file again: the "restarts cleanly" check.
    pub async fn restart(mut self, sync: Sync) -> Self {
        self.board.stop();
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        let dir = std::mem::replace(&mut self._dir, tempfile::tempdir().expect("temp dir"));
        let path = self.path.clone();
        drop(self);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        Self::open(dir, path, sync).await
    }

    /// Scrape `GET /metrics` with the token, as `name{labels}` to value.
    pub async fn metrics(&self) -> Option<std::collections::BTreeMap<String, f64>> {
        let base = self.url.trim_end_matches("/v1");
        let text = self
            .http
            .get(format!("{base}/metrics"))
            .bearer_auth(&self.token)
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        Some(
            text.lines()
                .filter(|l| !l.starts_with('#') && !l.is_empty())
                .filter_map(|l| {
                    let (k, v) = l.rsplit_once(' ')?;
                    Some((k.to_string(), v.parse().ok()?))
                })
                .collect(),
        )
    }

    pub fn wal_bytes(&self) -> u64 {
        let mut wal = self.path.clone().into_os_string();
        wal.push("-wal");
        std::fs::metadata(wal).map(|m| m.len()).unwrap_or(0)
    }
}

/// 1,000 waiters need more sockets than the usual soft limit allows.
fn raise_fd_limit() {
    #[cfg(unix)]
    unsafe {
        let mut l = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut l) == 0 && l.rlim_cur < l.rlim_max {
            l.rlim_cur = l.rlim_max.min(65_536);
            libc::setrlimit(libc::RLIMIT_NOFILE, &l);
        }
    }
}

/// Resident memory and open files of this process (Linux), for the soak run.
pub fn process_usage() -> (u64, u64) {
    let rss_kb = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0);
    let fds = std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count() as u64)
        .unwrap_or(0);
    (rss_kb, fds)
}
