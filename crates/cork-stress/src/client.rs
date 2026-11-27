//! A thin HTTP client for the scenarios. It reports error codes instead of
//! panicking, so a scenario can count them.

use serde_json::{Value, json};
use std::time::Duration;

#[derive(Clone)]
pub struct Client {
    base: String,
    token: String,
    profile: String,
    http: reqwest::Client,
}

/// A failed call: the stable error code, or `transport` for network errors.
#[derive(Debug, Clone, PartialEq)]
pub struct CallError {
    pub code: String,
    pub message: String,
}

pub type CallResult<T> = Result<T, CallError>;

pub struct Claimed {
    pub task_id: String,
    pub claim_id: String,
    pub expires_at: i64,
}

impl Client {
    pub fn new(base: &str, token: &str, profile: &str, http: reqwest::Client) -> Self {
        Self {
            base: base.to_string(),
            token: token.to_string(),
            profile: profile.to_string(),
            http,
        }
    }

    pub fn as_profile(&self, profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
            ..self.clone()
        }
    }

    async fn call(&self, m: reqwest::Method, path: &str, body: Option<Value>) -> CallResult<Value> {
        self.call_idem(m, path, body, None).await
    }

    async fn call_idem(
        &self,
        m: reqwest::Method,
        path: &str,
        body: Option<Value>,
        key: Option<&str>,
    ) -> CallResult<Value> {
        let mut r = self
            .http
            .request(m, format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .header("x-cork-profile", &self.profile)
            .timeout(Duration::from_secs(120));
        if let Some(k) = key {
            r = r.header("idempotency-key", k);
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = r.send().await.map_err(transport)?;
        let ok = resp.status().is_success();
        let v: Value = resp.json().await.map_err(transport)?;
        if ok {
            Ok(v)
        } else {
            Err(CallError {
                code: v["error"]["code"].as_str().unwrap_or("unknown").into(),
                message: v["error"]["message"].as_str().unwrap_or("").into(),
            })
        }
    }

    pub async fn get(&self, path: &str) -> CallResult<Value> {
        self.call(reqwest::Method::GET, path, None).await
    }

    pub async fn post(&self, path: &str, body: Value) -> CallResult<Value> {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    /// A POST made safe to retry with an idempotency key.
    pub async fn post_idem(&self, path: &str, body: Value, key: &str) -> CallResult<Value> {
        self.call_idem(reqwest::Method::POST, path, Some(body), Some(key))
            .await
    }

    pub async fn create(
        &self,
        channel: &str,
        text: &str,
        lease_ms: i64,
        max_attempts: u32,
    ) -> CallResult<String> {
        let v = self
            .post(
                "/tasks",
                json!({ "channel": channel, "text": text, "lease_ms": lease_ms, "max_attempts": max_attempts }),
            )
            .await?;
        Ok(v["task"]["id"].as_str().unwrap_or_default().to_string())
    }

    /// Claim the oldest ready task, waiting up to `wait_ms`. `None` means
    /// nothing was ready or the wait ran out.
    pub async fn claim(&self, channel: &str, wait_ms: u64) -> CallResult<Option<Claimed>> {
        let v = self
            .post(
                "/tasks/claim",
                json!({ "channel": channel, "wait_ms": wait_ms }),
            )
            .await?;
        if v["outcome"] != "claimed" {
            return Ok(None);
        }
        Ok(Some(Claimed {
            task_id: v["task"]["id"].as_str().unwrap_or_default().into(),
            claim_id: v["claim_id"].as_str().unwrap_or_default().into(),
            expires_at: v["expires_at"].as_i64().unwrap_or_default(),
        }))
    }

    pub async fn settle(&self, task_id: &str, op: &str, claim_id: &str) -> CallResult<Value> {
        self.post(
            &format!("/tasks/{task_id}/{op}"),
            json!({ "claim_id": claim_id, "reason": "stress" }),
        )
        .await
    }

    pub async fn post_message(
        &self,
        channel: &str,
        text: &str,
        label: (&str, &str),
    ) -> CallResult<Value> {
        let mut labels = serde_json::Map::new();
        labels.insert(label.0.into(), label.1.into());
        self.post(
            &format!("/channels/{}/messages", channel.replace('/', "%2F")),
            json!({ "text": text, "labels": labels }),
        )
        .await
    }

    /// Every event in the log, in order.
    pub async fn all_events(&self) -> CallResult<Vec<Value>> {
        let mut out = vec![];
        let mut after = 0;
        loop {
            let page = self
                .get(&format!("/events?after={after}&limit=1000"))
                .await?;
            let events = page["events"].as_array().cloned().unwrap_or_default();
            if events.is_empty() {
                return Ok(out);
            }
            after = page["next_after"].as_u64().unwrap_or(after);
            out.extend(events);
        }
    }
}

fn transport(e: reqwest::Error) -> CallError {
    CallError {
        code: "transport".into(),
        message: e.to_string(),
    }
}
