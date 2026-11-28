//! A real daemon on a loopback port, over a SQLite file, for API tests.
#![allow(dead_code)]

use cork_server::*;
use cork_store_sqlite::{Durability, SqliteConfig, SqliteStore};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;

pub const TOKEN: &str = "test-token";

pub struct Daemon {
    pub url: String,
    pub port: u16,
    pub board: Arc<Board<SqliteStore>>,
    pub path: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
    _dir: Option<tempfile::TempDir>,
}

impl Daemon {
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("board.db");
        let mut d = Self::open(path, None).await;
        d._dir = Some(dir);
        d
    }

    /// A fresh daemon that checks requests with `auth`.
    pub async fn start_with(auth: Arc<dyn Authenticator>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("board.db");
        let mut d = Self::open_with(path, None, auth).await;
        d._dir = Some(dir);
        d
    }

    /// Open (or reopen) a daemon over the file at `path`, on `port` if given.
    pub async fn open(path: PathBuf, port: Option<u16>) -> Self {
        Self::open_with(path, port, Arc::new(LocalToken::new(TOKEN, "chris"))).await
    }

    pub async fn open_with(path: PathBuf, port: Option<u16>, auth: Arc<dyn Authenticator>) -> Self {
        let mut cfg = SqliteConfig::new(&path);
        cfg.durability = Durability::Normal;
        let store = Arc::new(SqliteStore::open(cfg).await.unwrap());
        let floor = cork_store::Store::max_at(&*store).await.unwrap();
        let board = Board::start(
            store,
            Arc::new(SystemClock::anchored(floor)),
            BoardConfig::default(),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port.unwrap_or(0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = Arc::new(App {
            board: board.clone(),
            auth,
            hosts: HostPolicy::loopback(port),
            sessions: Default::default(),
        });
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            serve(listener, app, async {
                let _ = rx.await;
            })
            .await
            .unwrap();
        });
        Self {
            url: format!("http://127.0.0.1:{port}/v1"),
            port,
            board,
            path,
            stop: Some(tx),
            task: Some(task),
            _dir: None,
        }
    }

    /// Stop serving and release the store, keeping the file.
    pub async fn shutdown(mut self) -> (PathBuf, u16, Option<tempfile::TempDir>) {
        self.board.stop();
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.task.take() {
            t.abort();
            let _ = t.await;
        }
        let path = self.path.clone();
        let port = self.port;
        let dir = self._dir.take();
        drop(self);
        // Let the writer thread see its channel close.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        (path, port, dir)
    }

    pub fn client(&self, profile: &str) -> Client {
        Client {
            base: self.url.clone(),
            http: reqwest::Client::new(),
            profile: profile.to_string(),
            token: TOKEN.into(),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    pub base: String,
    pub http: reqwest::Client,
    pub profile: String,
    pub token: String,
}

/// A response: status and JSON body.
#[derive(Debug, Clone)]
pub struct Res {
    pub status: u16,
    pub body: Value,
}

impl Res {
    pub fn ok(self) -> Value {
        assert_eq!(self.status, 200, "{}", self.body);
        self.body
    }
    pub fn code(&self) -> &str {
        self.body["error"]["code"].as_str().unwrap_or("")
    }
}

impl Client {
    fn req(&self, m: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut r = self
            .http
            .request(m, format!("{}{path}", self.base))
            .bearer_auth(&self.token);
        if !self.profile.is_empty() {
            r = r.header("x-cork-profile", &self.profile);
        }
        r
    }

    async fn send(r: reqwest::RequestBuilder) -> Res {
        let resp = r.send().await.unwrap();
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap();
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Res { status, body }
    }

    pub async fn get(&self, path: &str) -> Res {
        Self::send(self.req(reqwest::Method::GET, path)).await
    }

    pub async fn post(&self, path: &str, body: Value) -> Res {
        Self::send(self.req(reqwest::Method::POST, path).json(&body)).await
    }

    pub async fn post_key(&self, path: &str, key: &str, body: Value) -> Res {
        Self::send(
            self.req(reqwest::Method::POST, path)
                .header("idempotency-key", key)
                .json(&body),
        )
        .await
    }

    pub async fn create(&self, channel: &str, text: &str, extra: Value) -> Value {
        let mut b = json!({ "channel": channel, "text": text });
        if let Value::Object(m) = extra {
            for (k, v) in m {
                b[k] = v;
            }
        }
        self.post("/tasks", b).await.ok()
    }

    pub async fn claim(&self, body: Value) -> Res {
        self.post("/tasks/claim", body).await
    }

    pub async fn settle(&self, id: &str, op: &str, body: Value) -> Res {
        self.post(&format!("/tasks/{id}/{op}"), body).await
    }

    pub async fn history_types(&self, id: &str) -> Vec<String> {
        self.get(&format!("/tasks/{id}/history")).await.ok()["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["type"].as_str().unwrap().to_string())
            .collect()
    }
}

/// All events in the log, in order.
pub async fn all_events(c: &Client) -> Vec<Value> {
    let mut out = vec![];
    let mut after = 0;
    loop {
        let page = c
            .get(&format!("/events?after={after}&limit=1000"))
            .await
            .ok();
        let events = page["events"].as_array().unwrap().clone();
        if events.is_empty() {
            return out;
        }
        after = page["next_after"].as_u64().unwrap();
        out.extend(events);
    }
}
