//! A client for the Cork HTTP API (HANDOFF 7), plus the client-side pieces
//! of the CLI: finding the daemon, starting it on demand (8.4), and claim
//! files (8.3).
//!
//! Clients never compare their own clock with `expires_at`; they use the
//! server's `now` from each response (13.5).

pub mod home;

use cork_api::*;
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

/// A failed call. `code` is the API's stable error code, or `transport`
/// when the daemon could not be reached.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientError {
    pub code: String,
    pub message: String,
}

impl ClientError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// The CLI exit code for this error (HANDOFF 8.6).
    pub fn exit_code(&self) -> i32 {
        match self.code.as_str() {
            "nothing_ready" => 3,
            "timeout" => 4,
            "claim_lost" => 5,
            "claim_not_held" => 6,
            "task_claimed" => 7,
            "usage" => 2,
            _ => 1,
        }
    }

    /// Worth retrying: the daemon may be restarting.
    pub fn is_transient(&self) -> bool {
        matches!(
            self.code.as_str(),
            "transport" | "unavailable" | "internal" | "unauthorized"
        )
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ClientError {}

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Clone)]
pub struct Client {
    base: String,
    token: String,
    profile: Option<String>,
    http: reqwest::Client,
}

/// Percent-encode one path segment (channels contain `/`).
pub fn segment(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Build a query string from pairs, encoding values.
pub fn query(pairs: &[(&str, String)]) -> String {
    let parts: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{k}={}", segment(v)))
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

impl Client {
    /// `url` is the daemon's base, such as `http://127.0.0.1:7420`.
    pub fn new(url: &str, token: &str, profile: Option<String>) -> Self {
        Self {
            base: format!("{}{PREFIX}", url.trim_end_matches('/')),
            token: token.to_string(),
            profile,
            http: reqwest::Client::new(),
        }
    }

    pub fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    async fn call(
        &self,
        m: reqwest::Method,
        path: &str,
        body: Option<Value>,
        key: Option<&str>,
        timeout: Duration,
    ) -> Result<Value> {
        let mut r = self
            .http
            .request(m, format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .timeout(timeout);
        if let Some(p) = &self.profile {
            r = r.header(HEADER_PROFILE, p);
        }
        if let Some(k) = key {
            r = r.header(HEADER_IDEMPOTENCY_KEY, k);
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = r
            .send()
            .await
            .map_err(|e| ClientError::new("transport", e.to_string()))?;
        let ok = resp.status().is_success();
        let text = resp
            .text()
            .await
            .map_err(|e| ClientError::new("transport", e.to_string()))?;
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        if ok {
            Ok(v)
        } else {
            Err(ClientError::new(
                v["error"]["code"].as_str().unwrap_or("internal"),
                v["error"]["message"]
                    .as_str()
                    .unwrap_or("unexpected response"),
            ))
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value> {
        self.call(
            reqwest::Method::GET,
            path,
            None,
            None,
            Duration::from_secs(30),
        )
        .await
    }

    /// A GET that may wait on the server (long poll), so it gets a longer timeout.
    pub async fn get_wait(&self, path: &str, wait: Duration) -> Result<Value> {
        self.call(
            reqwest::Method::GET,
            path,
            None,
            None,
            wait + Duration::from_secs(15),
        )
        .await
    }

    pub async fn post<B: Serialize>(
        &self,
        path: &str,
        body: &B,
        key: Option<&str>,
    ) -> Result<Value> {
        self.post_wait(path, body, key, Duration::ZERO).await
    }

    pub async fn post_wait<B: Serialize>(
        &self,
        path: &str,
        body: &B,
        key: Option<&str>,
        wait: Duration,
    ) -> Result<Value> {
        let body =
            serde_json::to_value(body).map_err(|e| ClientError::new("internal", e.to_string()))?;
        self.call(
            reqwest::Method::POST,
            path,
            Some(body),
            key,
            wait + Duration::from_secs(30),
        )
        .await
    }

    pub async fn health(&self) -> Result<Health> {
        let v = self
            .call(
                reqwest::Method::GET,
                "/health",
                None,
                None,
                Duration::from_secs(2),
            )
            .await?;
        serde_json::from_value(v).map_err(|e| ClientError::new("internal", e.to_string()))
    }

    pub async fn whoami(&self) -> Result<WhoAmI> {
        let v = self.get("/whoami").await?;
        serde_json::from_value(v).map_err(|e| ClientError::new("internal", e.to_string()))
    }

    pub async fn post_message(
        &self,
        channel: &str,
        m: &PostMessage,
        key: Option<&str>,
    ) -> Result<Value> {
        self.post(&format!("/channels/{}/messages", segment(channel)), m, key)
            .await
    }

    pub async fn reply(&self, id: &str, m: &PostMessage, key: Option<&str>) -> Result<Value> {
        self.post(&format!("/threads/{id}/replies"), m, key).await
    }

    pub async fn create_task(&self, req: &CreateTaskReq, key: Option<&str>) -> Result<Value> {
        self.post("/tasks", req, key).await
    }

    pub async fn create_plan(&self, req: &PlanReq, key: Option<&str>) -> Result<Value> {
        self.post("/tasks/plan", req, key).await
    }

    pub async fn claim(&self, req: &ClaimReq, key: Option<&str>) -> Result<ClaimOut> {
        let wait = Duration::from_millis(req.wait_ms.unwrap_or(0));
        let v = self.post_wait("/tasks/claim", req, key, wait).await?;
        serde_json::from_value(v).map_err(|e| ClientError::new("internal", e.to_string()))
    }

    pub async fn claim_raw(&self, req: &ClaimReq, key: Option<&str>) -> Result<Value> {
        let wait = Duration::from_millis(req.wait_ms.unwrap_or(0));
        self.post_wait("/tasks/claim", req, key, wait).await
    }

    pub async fn settle(
        &self,
        id: &str,
        op: &str,
        req: &SettleReq,
        key: Option<&str>,
    ) -> Result<Value> {
        self.post(&format!("/tasks/{id}/{op}"), req, key).await
    }

    pub async fn task(&self, id: &str) -> Result<Value> {
        self.get(&format!("/tasks/{id}")).await
    }

    pub async fn history(&self, id: &str) -> Result<Value> {
        self.get(&format!("/tasks/{id}/history")).await
    }

    pub async fn thread(&self, id: &str) -> Result<Value> {
        self.get(&format!("/threads/{id}?limit={THREAD_PAGE_MAX}"))
            .await
    }

    /// One long poll of `/events`. `params` are the filters.
    pub async fn events(&self, params: &[(&str, String)], wait: Duration) -> Result<EventsPage> {
        let v = self
            .get_wait(&format!("/events{}", query(params)), wait)
            .await?;
        serde_json::from_value(v).map_err(|e| ClientError::new("internal", e.to_string()))
    }

    /// The whole log as JSONL.
    pub async fn export(&self) -> Result<String> {
        let mut r = self
            .http
            .get(format!("{}/export", self.base))
            .bearer_auth(&self.token);
        if let Some(p) = &self.profile {
            r = r.header(HEADER_PROFILE, p);
        }
        let resp = r
            .send()
            .await
            .map_err(|e| ClientError::new("transport", e.to_string()))?;
        if !resp.status().is_success() {
            return Err(ClientError::new(
                "internal",
                format!("export failed: {}", resp.status()),
            ));
        }
        resp.text()
            .await
            .map_err(|e| ClientError::new("transport", e.to_string()))
    }
}
