//! `cork serve`: the portable daemon (HANDOFF 8.4, 10, 13.4).

use cork_client::home::{DaemonInfo, FileLock, daemon_file, read_daemon, write_private};
use cork_server::*;
use cork_store::{EventMeta, Store, StoredEvent};
use cork_store_sqlite::{Durability, SqliteConfig, SqliteStore};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub struct ServeOpts {
    pub home: PathBuf,
    pub port: u16,
    pub db: PathBuf,
    pub full_sync: bool,
    pub import: Option<PathBuf>,
    /// Label `/metrics` task gauges with channel names.
    pub metrics_channels: bool,
}

/// A random 256-bit token, hex.
pub fn new_token() -> std::io::Result<String> {
    let mut buf = [0u8; 32];
    std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut buf)?;
    Ok(hex::encode(buf))
}

/// The person the daemon acts for: `CORK_USER`, else the OS user (4).
pub fn person() -> Result<String, String> {
    let name = ["CORK_USER", "USER", "LOGNAME", "USERNAME"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
        .ok_or("set CORK_USER to your name")?;
    cork_core::actor::normalize_name("person", &name)
        .map_err(|e| format!("{}; set CORK_USER to a name of [A-Za-z0-9_.-]", e.message))
}

/// The token from `daemon.json`, re-read when a request brings a token we
/// do not know, so `cork token rotate` takes effect without a restart.
struct FileToken {
    home: PathBuf,
    person: String,
    token: RwLock<(String, Instant)>,
}

impl Authenticator for FileToken {
    fn authenticate(
        &self,
        headers: &cork_server::http_types::HeaderMap,
    ) -> cork_core::Result<String> {
        let current = self.token.read().unwrap().0.clone();
        if LocalToken::new(current, &self.person)
            .authenticate(headers)
            .is_ok()
        {
            return Ok(self.person.clone());
        }
        // Re-read at most once a second.
        let stale = self.token.read().unwrap().1.elapsed() > Duration::from_secs(1);
        if stale {
            if let Some(info) = read_daemon(&self.home) {
                *self.token.write().unwrap() = (info.token, Instant::now());
            } else {
                self.token.write().unwrap().1 = Instant::now();
            }
        }
        let current = self.token.read().unwrap().0.clone();
        LocalToken::new(current, &self.person).authenticate(headers)
    }

    /// A hash of the token now in `daemon.json`, re-read at most once a
    /// second, so `cork token rotate` ends web sessions (20.6).
    fn epoch(&self) -> String {
        let stale = self.token.read().unwrap().1.elapsed() > Duration::from_secs(1);
        if stale {
            let mut t = self.token.write().unwrap();
            if let Some(info) = read_daemon(&self.home) {
                t.0 = info.token;
            }
            t.1 = Instant::now();
        }
        hex::encode(Sha256::digest(self.token.read().unwrap().0.as_bytes()))
    }
}

pub async fn serve(o: ServeOpts) -> Result<(), String> {
    let dir =
        o.db.parent()
            .filter(|d| !d.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let _lock = FileLock::try_acquire(&dir.join("serve.lock"))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("another cork daemon is already serving {}", o.db.display()))?;
    let person = person()?;

    let mut cfg = SqliteConfig::new(&o.db);
    cfg.durability = if o.full_sync {
        Durability::Full
    } else {
        Durability::Normal
    };
    let store = Arc::new(SqliteStore::open(cfg).await.map_err(|e| e.message)?);
    if let Some(file) = &o.import {
        let events = read_jsonl(file)?;
        let n = events.len();
        store
            .import(events)
            .await
            .map_err(|e| format!("import: {}", e.message))?;
        eprintln!("cork: imported {n} events from {}", file.display());
    }
    let floor = store.max_at().await.map_err(|e| e.message)?;
    let canonical = std::fs::canonicalize(&o.db).unwrap_or(o.db.clone());
    let board_id = hex::encode(&Sha256::digest(canonical.to_string_lossy().as_bytes())[..8]);
    let board = Board::start(
        store,
        Arc::new(SystemClock::anchored(floor)),
        BoardConfig {
            id: board_id,
            name: "cork".into(),
            metrics_channels: o.metrics_channels,
            wal_path: Some({
                let mut w = o.db.clone().into_os_string();
                w.push("-wal");
                w.into()
            }),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| e.message)?;

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", o.port))
        .await
        .map_err(|e| format!("listen on 127.0.0.1:{}: {e}", o.port))?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();

    // Reuse the token across restarts, so running clients keep working (10).
    let token = match read_daemon(&o.home) {
        Some(i) => i.token,
        None => new_token().map_err(|e| e.to_string())?,
    };
    let info = DaemonInfo {
        url: format!("http://127.0.0.1:{port}"),
        port,
        token: token.clone(),
    };
    write_private(
        &daemon_file(&o.home),
        &serde_json::to_vec_pretty(&info).unwrap(),
    )
    .map_err(|e| e.to_string())?;

    let app = Arc::new(App {
        board: board.clone(),
        auth: Arc::new(FileToken {
            home: o.home.clone(),
            person,
            token: RwLock::new((token, Instant::now())),
        }),
        hosts: HostPolicy::loopback(port),
        sessions: Default::default(),
    });
    eprintln!("cork: serving {} on 127.0.0.1:{port}", o.db.display());
    let shutdown = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        eprintln!("cork: shutting down");
    };
    let r = cork_server::serve(listener, app, shutdown).await;
    board.stop();
    r.map_err(|e| e.to_string())
}

/// Read an export: one event envelope per line.
fn read_jsonl(path: &Path) -> Result<Vec<StoredEvent>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            let e: cork_api::EventOut =
                serde_json::from_str(l).map_err(|e| format!("line {}: {e}", i + 1))?;
            Ok(StoredEvent {
                position: e.position,
                stream_id: e.stream,
                version: e.version,
                event_type: e.event_type,
                schema: e.schema,
                data: e.data,
                actor: e.actor,
                at: e.at,
                meta: EventMeta {
                    channel: e.channel,
                    thread_id: e.thread_id,
                    labels: e.labels,
                },
            })
        })
        .collect()
}

/// `cork token rotate`: a new token in `daemon.json`. A running daemon picks
/// it up on the next request.
pub fn rotate(home: &Path) -> Result<(), String> {
    let mut info = read_daemon(home).ok_or("no daemon.json: start the daemon first")?;
    info.token = new_token().map_err(|e| e.to_string())?;
    write_private(
        &daemon_file(home),
        &serde_json::to_vec_pretty(&info).unwrap(),
    )
    .map_err(|e| e.to_string())
}

/// `cork debug rebuild --db <copy>`: rebuild projections from the log in a
/// copy and diff them against what the copy held.
pub async fn rebuild(db: &Path) -> Result<Vec<String>, String> {
    if !db.exists() {
        return Err(format!("{} does not exist", db.display()));
    }
    let dir = db
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let _lock = FileLock::try_acquire(&dir.join("serve.lock"))
        .map_err(|e| e.to_string())?
        .ok_or("a daemon is serving that database: rebuild a copy")?;
    let s = SqliteStore::open(SqliteConfig::new(db))
        .await
        .map_err(|e| e.message)?;
    let before = s.projection_dump().await.map_err(|e| e.message)?;
    s.rebuild_projections().await.map_err(|e| e.message)?;
    let after = s.projection_dump().await.map_err(|e| e.message)?;
    let b: std::collections::BTreeSet<_> = before.iter().collect();
    let a: std::collections::BTreeSet<_> = after.iter().collect();
    let mut diff: Vec<String> = b.difference(&a).map(|l| format!("- {l}")).collect();
    diff.extend(a.difference(&b).map(|l| format!("+ {l}")));
    Ok(diff)
}
